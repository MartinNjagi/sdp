package wallet

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sdp/data"

	_ "encoding/json"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"strconv"
)

const (
	keyBalance = "wallet:%s:balance"
	keyPending = "wallet:%s:pending_deduction"
	keyCount   = "wallet:%s:pending_count"
)

// HotWallet manages per-client message-credit balances in Redis using
// atomic Lua scripts. It is the single point of origin for all credit
// deductions in the SDP. Balances are integer credits (1 credit ≈ 1 SMS
// unit) — never currency. The Flusher periodically syncs accumulated
// deductions back to the Core Wallet Service's PostgreSQL ledger.
type HotWallet struct {
	rdc *redis.Client

	balanceURL string // Core Engine internal balance lookup endpoint
	httpClient *http.Client

	// Pre-loaded Lua script SHAs — loaded once at startup via SCRIPT LOAD,
	// then called with EVALSHA for maximum throughput.
	shaDeduct           string
	shaRefund           string
	shaFlushAccumulator string
	shaSeedBalance      string
}

// New constructs a HotWallet and pre-loads all Lua scripts into Redis.
func New(rdc *redis.Client, cfg *data.AppConfig, client *http.Client) (*HotWallet, error) {
	ctx := context.Background()

	shaDeduct, err := rdc.ScriptLoad(ctx, luaDeduct).Result()
	if err != nil {
		return nil, fmt.Errorf("hot wallet: load deduct script: %w", err)
	}
	shaRefund, err := rdc.ScriptLoad(ctx, luaRefund).Result()
	if err != nil {
		return nil, fmt.Errorf("hot wallet: load refund script: %w", err)
	}
	shaFlush, err := rdc.ScriptLoad(ctx, luaFlushAccumulator).Result()
	if err != nil {
		return nil, fmt.Errorf("hot wallet: load flush script: %w", err)
	}
	shaSeed, err := rdc.ScriptLoad(ctx, luaSeedBalance).Result()
	if err != nil {
		return nil, fmt.Errorf("hot wallet: load seed script: %w", err)
	}

	logrus.Info("[HotWallet] Lua scripts loaded ✅")

	WalletBalanceURL := cfg.WalletBalanceURL

	return &HotWallet{
		rdc:                 rdc,
		balanceURL:          WalletBalanceURL,
		httpClient:          client,
		shaDeduct:           shaDeduct,
		shaRefund:           shaRefund,
		shaFlushAccumulator: shaFlush,
		shaSeedBalance:      shaSeed,
	}, nil
}

// Deduct attempts to reserve credits for a message. If the cache is cold,
// it automatically pauses, fetches the live balance from the Core Wallet
// Service, seeds Redis, and retries the deduction.
func (w *HotWallet) Deduct(ctx context.Context, req data.DeductCreditRequest) (*data.DeductCreditResult, error) {
	clientIDStr := fmt.Sprintf("%v", req.ClientID)
	keys := []string{
		fmt.Sprintf(keyBalance, clientIDStr),
		fmt.Sprintf(keyPending, clientIDStr),
		fmt.Sprintf(keyCount, clientIDStr),
	}

	// 1. Attempt the fast-path deduction using your auto-reloading eval
	res, err := w.evalWithReload(ctx, &w.shaDeduct, luaDeduct, keys, req.Amount)
	if err != nil {
		return nil, fmt.Errorf("lua deduct error: %w", err)
	}

	// 2. Check for the "Cold Cache" signal (-1)
	if val, ok := res.(int64); ok && val == -1 {
		logrus.Infof("[HotWallet] Cold cache for client=%s. Fetching from Wallet Service...", clientIDStr)

		// Heal the cache by reading from the HTTP endpoint
		_, seedErr := w.ReadBalance(ctx, clientIDStr)
		if seedErr != nil {
			return nil, fmt.Errorf("failed to seed cold wallet cache: %w", seedErr)
		}

		// Retry the exact same Lua script now that Redis is fully warm
		res, err = w.evalWithReload(ctx, &w.shaDeduct, luaDeduct, keys, req.Amount)
		if err != nil {
			return nil, fmt.Errorf("lua deduct retry error: %w", err)
		}
	}

	// 3. Parse the final Lua array response: {SuccessCode, BalanceString}
	arr, ok := res.([]interface{})
	if !ok || len(arr) != 2 {
		return nil, fmt.Errorf("unexpected lua response format: %v", res)
	}

	successCode, ok1 := arr[0].(int64)
	balanceStr, ok2 := arr[1].(string)

	if !ok1 || !ok2 {
		return nil, fmt.Errorf("failed to type-assert lua array elements")
	}

	balanceAfter, err := strconv.ParseInt(balanceStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("failed to parse returned balance: %w", err)
	}

	return &data.DeductCreditResult{
		Success:      successCode == 1,
		BalanceAfter: balanceAfter,
	}, nil
}

// Refund atomically credits an amount back to the hot balance and
// decrements the accumulator. Called by the DLR reconciler on FAILED
// delivery when RefundOnFailedDelivery is enabled. Satisfies the
// dlr.WalletRefunder interface (amount is float64 there for currency-style
// callers; converted to int64 credits here).
func (w *HotWallet) Refund(ctx context.Context, clientID string, amount float64, _ string) error {
	credits := int64(amount)
	keys := []string{
		fmt.Sprintf(keyBalance, clientID),
		fmt.Sprintf(keyPending, clientID),
		fmt.Sprintf(keyCount, clientID),
	}

	_, err := w.evalWithReload(ctx, &w.shaRefund, luaRefund, keys, strconv.FormatInt(credits, 10))
	if err != nil {
		return fmt.Errorf("hot wallet: refund client=%s amount=%d: %w", clientID, credits, err)
	}

	logrus.Debugf("[HotWallet] Refunded %d credits to client=%s", credits, clientID)
	return nil
}

// SeedBalance sets or overwrites the hot balance (credits) for a client.
// force=true overwrites an existing balance; force=false is a no-op if a
// balance already exists.
func (w *HotWallet) SeedBalance(ctx context.Context, clientID string, amount int64, force bool) error {
	forceStr := "0"
	if force {
		forceStr = "1"
	}
	keys := []string{fmt.Sprintf(keyBalance, clientID)}

	_, err := w.evalWithReload(ctx, &w.shaSeedBalance, luaSeedBalance, keys,
		strconv.FormatInt(amount, 10), forceStr,
	)
	if err != nil {
		return fmt.Errorf("hot wallet: seed balance client=%s: %w", clientID, err)
	}

	logrus.Debugf("[HotWallet] Seeded balance client=%s amount=%d force=%v", clientID, amount, force)
	return nil
}

// ReadBalance returns the current hot balance (credits) for a client
// without modifying it. If no balance is cached, it falls back to a live
// lookup against the Core Engine's internal balance endpoint and seeds the
// cache, so a cold Redis never reports zero incorrectly.
func (w *HotWallet) ReadBalance(ctx context.Context, clientID string) (int64, error) {
	val, err := w.rdc.Get(ctx, fmt.Sprintf(keyBalance, clientID)).Result()
	if errors.Is(err, redis.Nil) {
		return w.fetchAndSeedBalance(ctx, clientID)
	}
	if err != nil {
		return 0, fmt.Errorf("hot wallet: read balance client=%s: %w", clientID, err)
	}
	return strconv.ParseInt(val, 10, 64)
}

// fetchAndSeedBalance calls the Core Engine's internal balance endpoint:
//
//	POST /api/v1/internal/wallet/balance
//
// and seeds the result into the hot wallet cache. Used whenever the SDP
// encounters a client with no cached balance — e.g. first message after
// a Redis restart, or a brand-new client.
func (w *HotWallet) fetchAndSeedBalance(ctx context.Context, clientID string) (int64, error) {
	if w.balanceURL == "" {
		return 0, fmt.Errorf("hot wallet: WALLET_BALANCE_URL not configured, cannot seed client=%s", clientID)
	}

	resp, err := w.queryBalance(ctx, clientID)
	if err != nil {
		return 0, err
	}

	if err := w.SeedBalance(ctx, clientID, resp.Balance, true); err != nil {
		logrus.Errorf("[HotWallet] Seed after fetch failed client=%s: %v", clientID, err)
		// Still return the fetched balance even if caching failed.
	}

	return resp.Balance, nil
}

// queryBalance performs the synchronous HTTP call to the Core Engine.
func (w *HotWallet) queryBalance(ctx context.Context, clientID string) (*data.WalletBalanceResponse, error) {
	body := data.BalanceCreditRequest{ClientID: clientID}
	payload, err := marshalJSON(body)
	if err != nil {
		return nil, fmt.Errorf("hot wallet: marshal balance request: %w", err)
	}

	req, err := newJSONRequest(ctx, w.balanceURL, payload)
	if err != nil {
		return nil, fmt.Errorf("hot wallet: build balance request: %w", err)
	}

	resp, err := w.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("hot wallet: balance request client=%s: %w", clientID, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return nil, fmt.Errorf("hot wallet: balance endpoint returned %d for client=%s", resp.StatusCode, clientID)
	}

	// Define the expected APIResponse envelope
	var envelope struct {
		Status  int                        `json:"status"`
		Message string                     `json:"message"`
		Data    data.WalletBalanceResponse `json:"data"`
	}

	if err := decodeJSON(resp.Body, &envelope); err != nil {
		return nil, fmt.Errorf("hot wallet: decode balance response client=%s: %w", clientID, err)
	}

	logrus.Infof("[HotWallet] Successfully seeded cache: client=%s balance=%d", clientID, envelope.Data.Balance)

	// Return the actual nested data
	return &envelope.Data, nil
}

// flushAccumulator atomically reads and resets the pending deduction for a
// client. Called exclusively by the Flusher.
func (w *HotWallet) flushAccumulator(ctx context.Context, clientID string) (amount int64, count int, err error) {
	keys := []string{
		fmt.Sprintf(keyPending, clientID),
		fmt.Sprintf(keyCount, clientID),
	}

	result, err := w.evalWithReload(ctx, &w.shaFlushAccumulator, luaFlushAccumulator, keys)
	if err != nil {
		return 0, 0, fmt.Errorf("hot wallet: flush accumulator client=%s: %w", clientID, err)
	}

	vals := result.([]interface{})
	amount, _ = strconv.ParseInt(vals[0].(string), 10, 64)
	countInt, _ := strconv.ParseInt(vals[1].(string), 10, 64)
	return amount, int(countInt), nil
}

// --------------------------------------------------------------------------
// Script execution with auto-reload on NOSCRIPT (Redis restart cleared cache)
// --------------------------------------------------------------------------

func (w *HotWallet) evalWithReload(ctx context.Context, sha *string, source string, keys []string, args ...interface{}) (interface{}, error) {
	result, err := w.rdc.EvalSha(ctx, *sha, keys, args...).Result()
	if err != nil && isNoScript(err) {
		newSha, loadErr := w.rdc.ScriptLoad(ctx, source).Result()
		if loadErr != nil {
			return nil, loadErr
		}
		*sha = newSha
		result, err = w.rdc.EvalSha(ctx, *sha, keys, args...).Result()
	}
	return result, err
}

func isNoScript(err error) bool {
	return err != nil && len(err.Error()) >= 8 && err.Error()[:8] == "NOSCRIPT"
}

func newJSONRequest(ctx context.Context, url string, payload []byte) (*http.Request, error) {
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		url,
		bytes.NewReader(payload),
	)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	return req, nil
}

func marshalJSON(v any) ([]byte, error) {
	return json.Marshal(v)
}

func decodeJSON(r io.Reader, v any) error {
	return json.NewDecoder(r).Decode(v)
}
