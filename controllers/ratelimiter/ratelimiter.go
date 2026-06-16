package ratelimiter

import (
	"context"
	"fmt"
	"sdp/data"
	"sync"

	"golang.org/x/time/rate"
)

// Limiter enforces per-MNO TPS ceilings using a token-bucket algorithm.
// Each MNO gets its own *rate.Limiter so traffic to Safaricom and Airtel
// is shaped independently. Buckets are created lazily on first use, and
// then updated if config changes (restart required currently).
type Limiter struct {
	mu         sync.RWMutex
	buckets    map[string]*rate.Limiter // keyed by MNO name
	tpsTable   map[string]int           // MNO name → configured TPS limit
	defaultTPS int
}

// New constructs the Limiter, pre-populating one bucket per configured MNO.
func New(cfg *data.AppConfig) *Limiter {
	l := &Limiter{
		buckets:    make(map[string]*rate.Limiter),
		tpsTable:   make(map[string]int),
		defaultTPS: 10, // conservative fallback if an MNO is not explicitly configured
	}

	for _, rc := range cfg.MNORoutes {
		tps := rc.TPSLimit
		if tps <= 0 {
			tps = l.defaultTPS
		}
		l.tpsTable[rc.Name] = tps
		// Burst = TPS so a single second's worth of messages can be sent
		// immediately on startup rather than dripping in one-by-one.
		l.buckets[rc.Name] = rate.NewLimiter(rate.Limit(tps), tps)
	}

	return l
}

// Wait blocks until a token is available for the named MNO, or until ctx
// is cancelled (e.g. on shutdown). The Worker calls this before every Send.
//
// If the MNO has no bucket yet (e.g. a new prefix added without restart),
// a bucket is created lazily using the default TPS.
func (l *Limiter) Wait(ctx context.Context, mno string) error {
	limiter := l.bucket(mno)
	if err := limiter.Wait(ctx); err != nil {
		return fmt.Errorf("ratelimiter: wait cancelled for mno=%s: %w", mno, err)
	}
	return nil
}

// SetTPS updates the TPS for a named MNO at runtime.
// Useful if a Telco calls you and asks you to back off — no restart needed.
func (l *Limiter) SetTPS(mno string, tps int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.tpsTable[mno] = tps
	l.buckets[mno] = rate.NewLimiter(rate.Limit(tps), tps)
}

// --------------------------------------------------------------------------
// Internal
// --------------------------------------------------------------------------

func (l *Limiter) bucket(mno string) *rate.Limiter {
	l.mu.RLock()
	b, ok := l.buckets[mno]
	l.mu.RUnlock()
	if ok {
		return b
	}

	// Lazy creation under write lock.
	l.mu.Lock()
	defer l.mu.Unlock()
	// Double-check — another goroutine may have created it while we waited.
	if b, ok = l.buckets[mno]; ok {
		return b
	}
	tps := l.defaultTPS
	if t, found := l.tpsTable[mno]; found {
		tps = t
	}
	b = rate.NewLimiter(rate.Limit(tps), tps)
	l.buckets[mno] = b
	return b
}
