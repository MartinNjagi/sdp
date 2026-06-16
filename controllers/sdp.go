package controllers

import (
	"context"
	"sdp/controllers/dispatcher"
	"sdp/controllers/dlr"
	"sdp/controllers/mno_router"
	"sdp/controllers/publisher"
	"sdp/controllers/ratelimiter"
	"sdp/controllers/worker"
	"sdp/data"

	amqplib "github.com/rabbitmq/amqp091-go"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

// SDP is the top-level Service Delivery Platform container.
// It owns the lifecycle of every sub-component and is the single
// value wired into App.Initialize via dependency injection.
type SDP struct {
	cfg        *data.AppConfig
	db         *gorm.DB
	publisher  *publisher.Publisher
	worker     *worker.Worker
	reconciler *dlr.Reconciler
}

// New constructs the fully-wired SDP.
// All subcomponents are built here and receive only what they need.
func New(
	ctx context.Context,
	cfg *data.AppConfig,
	db *gorm.DB,
	conn *amqplib.Connection,
) (*SDP, error) {

	// --- Dispatcher ---------------------------------------------------------
	// Swap NewSMPP for NewHTTP (or compose both) based on cfg.
	// We start with the HTTP dispatcher so the pipeline is testable immediately
	// without a live SMPP bind.
	disp, err := dispatcher.NewHTTP(cfg)
	if err != nil {
		return nil, err
	}

	// --- MNO Router ---------------------------------------------------------
	// Resolves an MSISDN prefix → MNORoute (name, dispatcher key, TPS ceiling).
	mnor, err := mno_router.New(cfg)
	if err != nil {
		return nil, err
	}

	// --- Rate Limiter -------------------------------------------------------
	// Per-MNO token-bucket; constructed from the same route config.
	rl := ratelimiter.New(cfg)

	// --- Publisher ----------------------------------------------------------
	// Declares the exchange + queue topology once, then exposes Publish/PublishBatch.
	pub, err := publisher.New(conn)
	if err != nil {
		return nil, err
	}

	// --- DLR Reconciler -----------------------------------------------------
	// Normalises raw Telco status codes → platform statuses, writes DB, triggers
	// refunds / SSE / client webhooks.
	rec := dlr.NewReconciler(db)

	// --- Worker -------------------------------------------------------------
	// Consume loop: broker → router → limiter → dispatcher → DB update.
	w, err := worker.New(ctx, cfg, conn, mnor, rl, disp, db)
	if err != nil {
		return nil, err
	}

	return &SDP{
		cfg:        cfg,
		db:         db,
		publisher:  pub,
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

// Start launches all background goroutines.
// Called once from App.Initialize after the HTTP server goroutine is running.
func (s *SDP) Start() {
	logrus.Info("[SDP] Starting worker pool...")
	s.worker.Start()
	logrus.Info("[SDP] ✅ All components running")
}

// Stop performs a graceful drain.
// Call this after srv.Shutdown() in main.go so in-flight messages are not lost.
func (s *SDP) Stop() {
	logrus.Warn("[SDP] Shutting down — draining in-flight messages...")
	s.worker.Stop()
	s.publisher.Close()
	logrus.Info("[SDP] ✅ Clean shutdown complete")
}
