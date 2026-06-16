package wallet

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sdp/data"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

const (
	// activeClientsKey is a Redis Set that tracks which clients had deductions
	// in the current flush window. Workers add to this set on every deduction
	// so the flusher knows exactly which accumulators to drain — no SCAN needed.
	activeClientsKey = "wallet:active_clients"
)

// Flusher runs a background ticker that drains the per-client pending
// accumulators and ships a single batched HTTP POST to the Core Wallet
// Service every FlushInterval. This decouples the hot path (Lua deductions
// at 10k TPS) from the cold path (PostgreSQL ledger writes).
type Flusher struct {
	wallet        *HotWallet
	rdc           *redis.Client
	walletSvcURL  string
	flushInterval time.Duration
	httpClient    *http.Client
}

// NewFlusher constructs a Flusher. walletSvcURL is the Core Wallet Service
// endpoint that accepts a WalletFlushPayload POST.
func NewFlusher(wallet *HotWallet, rdc *redis.Client, walletSvcURL string, interval int) *Flusher {
	return &Flusher{
		wallet:        wallet,
		rdc:           rdc,
		walletSvcURL:  walletSvcURL,
		flushInterval: time.Duration(interval) * time.Second,
		httpClient:    &http.Client{Timeout: 10 * time.Second},
	}
}

// Start launches the flush loop. Blocks until ctx is cancelled — run in a goroutine.
func (f *Flusher) Start(ctx context.Context) {
	ticker := time.NewTicker(f.flushInterval)
	defer ticker.Stop()

	logrus.Infof("[Flusher] Started — flush interval=%s", f.flushInterval)

	for {
		select {
		case <-ctx.Done():
			// Final flush on shutdown so no deductions are lost.
			logrus.Info("[Flusher] Context cancelled — performing final flush...")
			f.flush(context.Background())
			logrus.Info("[Flusher] Final flush complete ✅")
			return

		case <-ticker.C:
			f.flush(ctx)
		}
	}
}

// TrackActiveClient records that a client had a deduction in the current
// window. Called by the DispatchWorker immediately after a successful Deduct.
// Using a Redis Set means duplicates are free — SADD on an existing member
// is a no-op.
func (f *Flusher) TrackActiveClient(ctx context.Context, clientID string) {
	if err := f.rdc.SAdd(ctx, activeClientsKey, clientID).Err(); err != nil {
		logrus.Warnf("[Flusher] TrackActiveClient client=%s: %v", clientID, err)
	}
}

// --------------------------------------------------------------------------
// Internal flush logic
// --------------------------------------------------------------------------

func (f *Flusher) flush(ctx context.Context) {
	// 1. Atomically pop the entire active clients set.
	//    SMEMBERS + DEL in a pipeline — the window resets cleanly.
	pipe := f.rdc.Pipeline()
	membersCmd := pipe.SMembers(ctx, activeClientsKey)
	pipe.Del(ctx, activeClientsKey)
	if _, err := pipe.Exec(ctx); err != nil {
		logrus.Errorf("[Flusher] Read active clients: %v", err)
		return
	}

	clients, err := membersCmd.Result()
	if err != nil || len(clients) == 0 {
		return // Nothing to flush
	}

	// 2. Drain each client's accumulator.
	var entries []data.WalletFlushEntry
	for _, clientID := range clients {
		amount, count, err := f.wallet.flushAccumulator(ctx, clientID)
		if err != nil {
			logrus.Errorf("[Flusher] Flush accumulator client=%s: %v", clientID, err)
			// Re-add to active set so next tick picks it up.
			f.rdc.SAdd(ctx, activeClientsKey, clientID)
			continue
		}
		if amount <= 0 {
			continue
		}
		entries = append(entries, data.WalletFlushEntry{
			ClientID:     clientID,
			Amount:       amount,
			MessageCount: count,
		})
	}

	if len(entries) == 0 {
		return
	}

	// 3. Ship a single HTTP POST to the Core Wallet Service.
	if err := f.sendBatch(ctx, entries); err != nil {
		logrus.Errorf("[Flusher] Batch send failed: %v — will retry next tick", err)
		// Re-seed accumulators so deductions are not lost.
		f.requeue(ctx, entries)
		return
	}

	total := int64(0)
	msgs := 0
	for _, e := range entries {
		total += e.Amount
		msgs += e.MessageCount
	}
	logrus.Infof("[Flusher] Flushed %d credits across %d messages for %d client(s)",
		total, msgs, len(entries))
}

func (f *Flusher) sendBatch(ctx context.Context, entries []data.WalletFlushEntry) error {
	payload := data.WalletFlushPayload{Entries: entries}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal flush payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, f.walletSvcURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build flush request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := f.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("http post: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("wallet service returned %d", resp.StatusCode)
	}
	return nil
}

// requeue adds deductions back into the accumulators so they are retried
// on the next flush tick. Called when the HTTP batch send fails.
func (f *Flusher) requeue(ctx context.Context, entries []data.WalletFlushEntry) {
	for _, e := range entries {
		keys := []string{
			fmt.Sprintf(keyPending, e.ClientID),
			fmt.Sprintf(keyCount, e.ClientID),
		}
		// Re-increment the accumulator — use plain INCRBYFLOAT, not the Lua
		// script, since we are adding back, not deducting from balance.
		if err := f.rdc.IncrBy(ctx, keys[0], e.Amount).Err(); err != nil {
			logrus.Errorf("[Flusher] Requeue pending client=%s: %v", e.ClientID, err)
		}
		if err := f.rdc.IncrBy(ctx, keys[1], int64(e.MessageCount)).Err(); err != nil {
			logrus.Errorf("[Flusher] Requeue count client=%s: %v", e.ClientID, err)
		}
		// Re-add to active set for next tick.
		f.rdc.SAdd(ctx, activeClientsKey, e.ClientID)
	}
}
