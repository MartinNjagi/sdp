package controllers

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"net/http"
	"sdp/connections"
	"sdp/controllers/dispatcher"
	"sdp/controllers/dlr"
	"sdp/controllers/mno_router"
	"sdp/controllers/publisher"
	"sdp/controllers/ratelimiter"
	"sdp/controllers/storage"
	"sdp/controllers/wallet"
	"sdp/controllers/worker"
	"sdp/data"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

// SDP is the top-level Service Delivery Platform container.
// It owns the lifecycle of every sub-component and is the single value
// wired into App.Initialize via dependency injection.
type SDP struct {
	ctx        context.Context
	cfg        *data.AppConfig
	db         *gorm.DB
	rdc        *redis.Client
	publisher  *publisher.Publisher
	hotWallet  *wallet.HotWallet
	flusher    *wallet.Flusher
	worker     *worker.Worker
	reconciler *dlr.Reconciler
}

// New constructs the fully-wired SDP. All sub-components are built here
// and receive only what they need — no global state, no package-level vars.
func New(
	ctx context.Context,
	cfg *data.AppConfig,
	db *gorm.DB,
	rdc *redis.Client,
	rmq *connections.RMQManager,
	rawStorageClient *s3.Client,
	client *http.Client,
) (*SDP, error) {

	// --- Publisher -----------------------------------------------------
	// Declares the three-queue topology, exposes typed publish methods.
	pub, err := publisher.New(rmq.Conn)
	if err != nil {
		return nil, fmt.Errorf("sdp: publisher: %w", err)
	}

	// --- Hot Wallet ------------------------------------------------------
	// Atomic Redis deductions via preloaded Lua scripts. Falls back to a
	// live lookup against WALLET_BALANCE_URL when a client has no cached
	// balance yet (cold Redis, brand-new client).
	hotWallet, err := wallet.New(rdc, cfg, client)
	if err != nil {
		return nil, fmt.Errorf("sdp: hot wallet: %w", err)
	}

	// --- Flusher -----------------------------------------------------
	// Batches accumulated deductions and syncs to the Core Wallet Service
	// every cfg.WalletFlushInterval.
	if cfg.WalletServiceURL == "" {
		logrus.Warn("[SDP] WALLET_SERVICE_URL is not set — flush batches will fail")
	}
	flusher := wallet.NewFlusher(hotWallet, rdc, cfg.WalletServiceURL, cfg.WalletFlushInterval, client)

	// --- Dispatcher ---------------------------------------------------------
	// SafaricomDispatcher needs a token getter closure — reads the cached
	// Bearer token from Redis, set by whatever auth-refresh job populates it.
	tokenGetter := func() string {
		val, err := rdc.Get(ctx, cfg.SDPTokenRedisKey).Result()
		if err != nil {
			logrus.Warnf("[SDP] Failed to read SDP token from redis key=%s: %v", cfg.SDPTokenRedisKey, err)
			return ""
		}
		return val
	}

	disp, err := dispatcher.NewSafaricom(cfg, tokenGetter)
	if err != nil {
		return nil, fmt.Errorf("sdp: safaricom dispatcher: %w", err)
	}

	// --- MNO Router ---------------------------------------------------------
	// Resolves an MSISDN prefix → MNORoute (name, dispatcher key, TPS ceiling).
	mnor, err := mno_router.New(cfg)
	if err != nil {
		return nil, fmt.Errorf("sdp: mno router: %w", err)
	}

	// --- Rate Limiter -------------------------------------------------------
	// Per-MNO token-bucket; constructed from the same route config.
	rl := ratelimiter.New(cfg)

	// --- DLR Reconciler -----------------------------------------------------
	// Normalises raw Telco status codes → platform statuses, writes DB,
	// triggers refunds / SSE / client webhooks.
	// WalletRefunder is the HotWallet itself — it satisfies the interface.
	// SSENotifier is a plain Redis PUBLISH to cfg.SSEChannel — the Node BFF
	// already subscribes to this channel and routes events to connected
	// WebSocket/SSE clients, so no further plumbing is needed here.
	sseNotifier := dlr.NewRedisSSENotifier(rdc, cfg.SSEChannel)
	rec := dlr.NewReconciler(db).
		WithWalletRefunder(hotWallet).
		WithSSENotifier(sseNotifier)
	// WebhookFirer (client-configured outbound webhooks) is attached by
	// App.Initialize once that service exists — see router/app.go.

	s3Svc := storage.NewS3Service(rawStorageClient)

	costEngine := data.NewCostEngine(nil, 10)

	// --- Worker ---------------------------------------------------------
	// Three sub-pools: bulk fan-out, VIP dispatch, standard dispatch.
	// Every shared collaborator is bundled into a single worker.Deps value
	// so New's signature stays short even as dependencies grow.
	deps := worker.Deps{
		RMQManager: rmq,
		Publisher:  pub,
		Router:     mnor,
		Limiter:    rl,
		Dispatcher: disp,
		HotWallet:  hotWallet,
		Flusher:    flusher,
		DB:         db,
		CostEngine: costEngine,
		S3:         s3Svc,
	}
	w, err := worker.New(ctx, cfg, deps)
	if err != nil {
		return nil, fmt.Errorf("sdp: worker: %w", err)
	}

	return &SDP{
		ctx:        ctx,
		cfg:        cfg,
		db:         db,
		rdc:        rdc,
		publisher:  pub,
		hotWallet:  hotWallet,
		flusher:    flusher,
		worker:     w,
		reconciler: rec,
	}, nil
}

// Publisher exposes the publisher so the Go Engine (/launch, Scheduler)
// can enqueue messages without importing the sub-package directly.
func (s *SDP) Publisher() *publisher.Publisher {
	return s.publisher
}

// Reconciler exposes the DLR reconciler so the Gin DLR webhook handler
// (registered in router/app.go) can call it directly.
func (s *SDP) Reconciler() *dlr.Reconciler {
	return s.reconciler
}

// HotWallet exposes the wallet so admin/balance-check endpoints
// (e.g. GET /api/v1/wallet/:client_id) can read live balances.
func (s *SDP) HotWallet() *wallet.HotWallet {
	return s.hotWallet
}

// Start launches all background goroutines: the three worker sub-pools
// and the wallet flush ticker. Called once from App.Initialize after the
// HTTP server goroutine is running, so DLR webhook routes are live before
// any worker begins consuming.
func (s *SDP) Start() {
	logrus.Info("[SDP] Starting worker pools and wallet flusher...")
	s.worker.Start(s.ctx)
	logrus.Info("[SDP] ✅ All components running")
}

// Stop performs a graceful drain of the entire SDP component: workers finish
// in-flight messages and ACK/NACK them, then the flusher performs one final
// flush so no pending deductions are lost, and finally the publisher is closed.
func (s *SDP) Stop() {
	logrus.Warn("[SDP] Shutting down — draining in-flight messages...")

	// 1. Drain workers (this ensures all final deductions hit Redis)
	s.worker.Stop()

	// 2. Force a final flush so no pending deductions are left behind in Redis
	logrus.Info("[SDP] Forcing final wallet flush...")

	// Use a 5-second timeout context specifically for this final network call
	flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s.flusher.ForceFlush(flushCtx)

	// 3. Close the publisher connection
	s.publisher.Close()

	logrus.Info("[SDP] ✅ Clean shutdown complete")
}
