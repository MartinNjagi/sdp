package wallet

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sdp/data"
	"strconv"
	"time"

	"github.com/google/uuid"
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
// accumulators and ships HTTP POSTs to the Core Wallet Service
// every FlushInterval. This decouples the hot path (Lua deductions
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
func NewFlusher(wallet *HotWallet, rdc *redis.Client, walletSvcURL string, interval int, client *http.Client) *Flusher {
	return &Flusher{
		wallet:        wallet,
		rdc:           rdc,
		walletSvcURL:  walletSvcURL,
		flushInterval: time.Duration(interval) * time.Second,
		httpClient:    client,
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

func (f *Flusher) ForceFlush(ctx context.Context) {
	f.flush(ctx)
}

// --------------------------------------------------------------------------
// Internal flush logic
// --------------------------------------------------------------------------

func (f *Flusher) flush(ctx context.Context) {

	// 0. PROCESS RETRIES FIRST
	for {
		// Pop from the right (oldest first)
		res, err := f.rdc.RPop(ctx, "wallet:flush_retries").Result()
		if err == redis.Nil {
			break // Queue is empty
		} else if err != nil {
			logrus.Errorf("[Flusher] Failed to read retry queue: %v", err)
			break
		}

		var retryEntry data.WalletFlushEntry
		_ = json.Unmarshal([]byte(res), &retryEntry)

		retryable, sendErr := f.sendEntry(ctx, retryEntry)
		if sendErr != nil {
			if retryable {
				logrus.Warnf("[Flusher] Retry failed for FlushID=%s: %v. Re-queueing.", retryEntry.FlushID, sendErr)
				// Put it back at the front of the line so we don't lose it
				f.rdc.RPush(ctx, "wallet:flush_retries", res)
				break // Stop processing retries; network is clearly still down
			} else {
				logrus.Errorf("[Flusher] 🚨 TERMINAL RETRY ERROR: Dropping payload %s: %v", retryEntry.FlushID, sendErr)
			}
		} else {
			logrus.Infof("[Flusher] Successfully recovered and flushed retry entry: %s", retryEntry.FlushID)
		}
	}

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
			FlushID:      "FLUSH_" + uuid.NewString(), // 👈 GENERATED ONCE HERE
			ClientID:     clientID,
			Amount:       amount,
			MessageCount: count,
		})
	}

	if len(entries) == 0 {
		return
	}

	// 3. Ship HTTP POSTs to the Core Wallet Service per entry.
	total := int64(0)
	msgs := 0
	successCount := 0

	for _, entry := range entries {
		retryable, err := f.sendEntry(ctx, entry)

		if err != nil {
			if retryable {
				logrus.Warnf("[Flusher] Send failed for client=%s: %v — will retry next tick", entry.ClientID, err)
				f.requeue(ctx, entry)
			} else {
				// TERMINAL ERROR. Drop it so it doesn't block the queue forever.
				// (In a production system, you might want to write this 'entry' to a dead-letter log or DB table here).
				logrus.Errorf("[Flusher] 🚨 TERMINAL SEND ERROR: Dropping payload for client=%s: %v", entry.ClientID, err)
			}
			continue
		}

		total += entry.Amount
		msgs += entry.MessageCount
		successCount++
	}

	if successCount > 0 {
		logrus.Infof("[Flusher] Flushed %d credits across %d messages for %d client(s)",
			total, msgs, successCount)
	}
}

// sendEntry ships a single flush entry and returns whether a failure should be retried.
func (f *Flusher) sendEntry(ctx context.Context, entry data.WalletFlushEntry) (retryable bool, err error) {
	// Convert string ClientID to uint for the Wallet Service struct
	clientIDUint, err := strconv.ParseUint(entry.ClientID, 10, 32)
	if err != nil {
		return false, fmt.Errorf("invalid client ID: %w", err)
	}

	// 🪄 THE HIJACK: We map our Flush payload to the DeductCreditRequest struct.
	// The Wallet Service will see CampaignID: "FLUSH_abcd..." and use it for idempotency.
	payload := data.DeductCreditPayload{
		ClientID:   uint(clientIDUint),
		Amount:     uint(entry.Amount),
		CampaignID: entry.FlushID,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return false, fmt.Errorf("marshal flush payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, f.walletSvcURL, bytes.NewReader(body))
	if err != nil {
		return false, fmt.Errorf("build flush request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := f.httpClient.Do(req)
	if err != nil {
		return true, fmt.Errorf("http post network error: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode <= 299 {
		return false, nil // Success
	}

	respBody, _ := io.ReadAll(resp.Body)
	serverMsg := string(respBody)

	if resp.StatusCode >= 400 && resp.StatusCode < 500 {
		return false, fmt.Errorf("wallet service rejected (HTTP %d): %s", resp.StatusCode, serverMsg)
	}

	return true, fmt.Errorf("wallet service unavailable (HTTP %d): %s", resp.StatusCode, serverMsg)
}

// requeue saves the exact failed payload (with its original FlushID) into a
// Redis list so it can be retried identically on the next tick.
func (f *Flusher) requeue(ctx context.Context, entry data.WalletFlushEntry) {
	dataBytes, err := json.Marshal(entry)
	if err != nil {
		logrus.Errorf("[Flusher] Failed to marshal retry entry: %v", err)
		return
	}

	// Push to a dedicated retry queue
	if err := f.rdc.LPush(ctx, "wallet:flush_retries", dataBytes).Err(); err != nil {
		logrus.Errorf("[Flusher] Failed to push to retry queue: %v", err)
	}
}
