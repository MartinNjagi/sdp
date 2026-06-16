package wallet

import (
	"context"
	"fmt"
	"sdp/data"
	"strconv"

	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

const (
	keyBalance = "wallet:%s:balance"
	keyPending = "wallet:%s:pending_deduction"
	keyCount   = "wallet:%s:pending_count"
)

// HotWallet manages per-client balances in Redis using atomic Lua scripts.
// It is the single point of origin for all financial deductions in the SDP.
// The flusher (flusher.go) periodically syncs accumulated deductions back
// to the Core Wallet Service's PostgreSQL ledger.
type HotWallet struct {
	rdc *redis.Client

	// Pre-loaded Lua script SHAs — loaded once at startup via SCRIPT LOAD,
	// then called with EVALSHA for maximum throughput.
	shaDeduct           string
	shaRefund           string
	shaFlushAccumulator string
	shaSeedBalance      string
}

// New constructs a HotWallet and pre-loads all Lua scripts into Redis.
// Pre-loading converts scripts to SHAs so subsequent calls use EVALSHA
// (cheaper than EVAL on every hot path).
func New(rdc *redis.Client) (*HotWallet, error) {
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

	return &HotWallet{
		rdc:                 rdc,
		shaDeduct:           shaDeduct,
		shaRefund:           shaRefund,
		shaFlushAccumulator: shaFlush,
		shaSeedBalance:      shaSeed,
	}, nil
}

// Deduct atomically checks and deducts the message cost from the client's
// hot balance. Returns DeductCreditResult.Success=false if insufficient funds.
// This is called by the DispatchWorker before every Send — it is on the
// critical path and must be as fast as possible.
func (w *HotWallet) Deduct(ctx context.Context, req data.DeductCreditRequest) (*data.DeductCreditResult, error) {
	keys := []string{
		fmt.Sprintf(keyBalance, req.ClientID),
		fmt.Sprintf(keyPending, req.ClientID),
		fmt.Sprintf(keyCount, req.ClientID),
	}

	result, err := w.rdc.EvalSha(ctx, w.shaDeduct, keys, fmt.Sprintf("%f", req.Amount)).Result()
	if err != nil {
		// If the SHA is missing (Redis restart cleared scripts), reload and retry once.
		if isNoScript(err) {
			if reloadErr := w.reloadScripts(ctx); reloadErr != nil {
				return nil, fmt.Errorf("hot wallet: reload scripts: %w", reloadErr)
			}
			result, err = w.rdc.EvalSha(ctx, w.shaDeduct, keys, fmt.Sprintf("%f", req.Amount)).Result()
		}
		if err != nil {
			return nil, fmt.Errorf("hot wallet: deduct client=%s: %w", req.ClientID, err)
		}
	}

	vals := result.([]interface{})
	success := vals[0].(int64) == 1
	balanceAfter, _ := strconv.ParseFloat(vals[1].(string), 64)

	if !success {
		logrus.Warnf("[HotWallet] Insufficient funds client=%s required=%.4f balance=%.4f",
			req.ClientID, req.Amount, balanceAfter)
	}

	return &data.DeductCreditResult{
		Success:      success,
		BalanceAfter: balanceAfter,
	}, nil
}

// Refund atomically credits an amount back to the hot balance and
// decrements the accumulator. Called by the DLR reconciler on FAILED
// delivery when RefundOnFailedDelivery is enabled.
func (w *HotWallet) Refund(ctx context.Context, clientID string, amount float64, _ string) error {
	keys := []string{
		fmt.Sprintf(keyBalance, clientID),
		fmt.Sprintf(keyPending, clientID),
		fmt.Sprintf(keyCount, clientID),
	}

	_, err := w.rdc.EvalSha(ctx, w.shaRefund, keys, fmt.Sprintf("%f", amount)).Result()
	if err != nil {
		if isNoScript(err) {
			if reloadErr := w.reloadScripts(ctx); reloadErr != nil {
				return fmt.Errorf("hot wallet: reload scripts: %w", reloadErr)
			}
			_, err = w.rdc.EvalSha(ctx, w.shaRefund, keys, fmt.Sprintf("%f", amount)).Result()
		}
		if err != nil {
			return fmt.Errorf("hot wallet: refund client=%s amount=%.4f: %w", clientID, amount, err)
		}
	}

	logrus.Debugf("[HotWallet] Refunded %.4f to client=%s", amount, clientID)
	return nil
}

// SeedBalance sets or overwrites the hot balance for a client.
// Called by the Core Wallet Service push (e.g. on top-up) or during
// initial cache warm-up on SDP startup.
// force=true overwrites an existing balance; force=false is a no-op if balance exists.
func (w *HotWallet) SeedBalance(ctx context.Context, clientID string, amount float64, force bool) error {
	forceStr := "0"
	if force {
		forceStr = "1"
	}

	keys := []string{fmt.Sprintf(keyBalance, clientID)}
	_, err := w.rdc.EvalSha(ctx, w.shaSeedBalance, keys,
		fmt.Sprintf("%f", amount), forceStr,
	).Result()
	if err != nil {
		if isNoScript(err) {
			if reloadErr := w.reloadScripts(ctx); reloadErr != nil {
				return fmt.Errorf("hot wallet: reload scripts: %w", reloadErr)
			}
			_, err = w.rdc.EvalSha(ctx, w.shaSeedBalance, keys,
				fmt.Sprintf("%f", amount), forceStr,
			).Result()
		}
		if err != nil {
			return fmt.Errorf("hot wallet: seed balance client=%s: %w", clientID, err)
		}
	}

	logrus.Debugf("[HotWallet] Seeded balance client=%s amount=%.4f force=%v", clientID, amount, force)
	return nil
}

// ReadBalance returns the current hot balance for a client without modifying it.
// Used for dashboard display and pre-flight checks by the Core Engine.
func (w *HotWallet) ReadBalance(ctx context.Context, clientID string) (float64, error) {
	val, err := w.rdc.Get(ctx, fmt.Sprintf(keyBalance, clientID)).Result()
	if err == redis.Nil {
		return 0, nil // No hot wallet seeded yet — treat as zero
	}
	if err != nil {
		return 0, fmt.Errorf("hot wallet: read balance client=%s: %w", clientID, err)
	}
	return strconv.ParseFloat(val, 64)
}

// flushAccumulator atomically reads and resets the pending deduction for a
// client. Called exclusively by the Flusher — not part of the public API.
func (w *HotWallet) flushAccumulator(ctx context.Context, clientID string) (amount float64, count int, err error) {
	keys := []string{
		fmt.Sprintf(keyPending, clientID),
		fmt.Sprintf(keyCount, clientID),
	}

	result, err := w.rdc.EvalSha(ctx, w.shaFlushAccumulator, keys).Result()
	if err != nil {
		if isNoScript(err) {
			if reloadErr := w.reloadScripts(ctx); reloadErr != nil {
				return 0, 0, reloadErr
			}
			result, err = w.rdc.EvalSha(ctx, w.shaFlushAccumulator, keys).Result()
		}
		if err != nil {
			return 0, 0, fmt.Errorf("hot wallet: flush accumulator client=%s: %w", clientID, err)
		}
	}

	vals := result.([]interface{})
	amount, _ = strconv.ParseFloat(vals[0].(string), 64)
	countInt, _ := strconv.ParseInt(vals[1].(string), 10, 64)
	return amount, int(countInt), nil
}

// --------------------------------------------------------------------------
// Script reload — handles Redis restarts clearing the script cache
// --------------------------------------------------------------------------

func (w *HotWallet) reloadScripts(ctx context.Context) error {
	var err error
	w.shaDeduct, err = w.rdc.ScriptLoad(ctx, luaDeduct).Result()
	if err != nil {
		return err
	}
	w.shaRefund, err = w.rdc.ScriptLoad(ctx, luaRefund).Result()
	if err != nil {
		return err
	}
	w.shaFlushAccumulator, err = w.rdc.ScriptLoad(ctx, luaFlushAccumulator).Result()
	if err != nil {
		return err
	}
	w.shaSeedBalance, err = w.rdc.ScriptLoad(ctx, luaSeedBalance).Result()
	return err
}

func isNoScript(err error) bool {
	return err != nil && len(err.Error()) >= 8 && err.Error()[:8] == "NOSCRIPT"
}
