package controllers

import (
	"context"
	"sdp/data"
	"sdp/dispatcher"
	"sdp/queue"

	amqplib "github.com/rabbitmq/amqp091-go"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

// Controller is the top-level Service Delivery Platform container.
// It owns the lifecycle of every subcomponent and is the single
// value wired into App.Initialize via dependency injection.
type Controller struct {
	cfg        *data.AppConfig
	db         *gorm.DB
	publisher  *queue.Publisher
	worker     *queue.Worker
	reconciler *queue.Reconciler
}

// NewController constructs the fully-wired SDP.
// All subcomponents are built here and receive only what they need.
func NewController(
	ctx context.Context,
	cfg *data.AppConfig,
	db *gorm.DB,
	conn *amqplib.Connection,
) (*Controller, error) {

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
	mnor, err := queue.NewMNORouter(cfg)
	if err != nil {
		return nil, err
	}

	// --- Rate Limiter -------------------------------------------------------
	// Per-MNO token-bucket; constructed from the same route config.
	rl := queue.NewLimiter(cfg)

	// --- Publisher ----------------------------------------------------------
	// Declares the exchange + queue topology once, then exposes Publish/PublishBatch.
	pub, err := queue.NewPublisher(conn)
	if err != nil {
		return nil, err
	}

	// --- DLR Reconciler -----------------------------------------------------
	// Normalises raw Telco status codes → platform statuses, writes DB, triggers
	// refunds / SSE / client webhooks.
	rec := queue.NewReconciler(db)

	// --- Worker -------------------------------------------------------------
	// Consume loop: broker → router → limiter → dispatcher → DB update.
	w, err := queue.NewWorker(ctx, cfg, conn, mnor, rl, disp, db)
	if err != nil {
		return nil, err
	}

	return &Controller{
		cfg:        cfg,
		db:         db,
		publisher:  pub,
		worker:     w,
		reconciler: rec,
	}, nil
}

// Publisher exposes the publisher so the Go Engine (/launch, Scheduler)
// can enqueue messages without importing the sub-package directly.
func (ctr *Controller) Publisher() *queue.Publisher {
	return ctr.publisher
}

// Reconciler exposes the DLR reconciler so the Gin DLR webhook handler
// (registered in router/app.go) can call it directly.
func (ctr *Controller) Reconciler() *queue.Reconciler {
	return ctr.reconciler
}

// Start launches all background goroutines.
// Called once from App.Initialize after the HTTP server goroutine is running.
func (ctr *Controller) Start() {
	logrus.Info("[SDP] Starting worker pool...")
	ctr.worker.Start()
	logrus.Info("[SDP] ✅ All components running")
}

// Stop performs a graceful drain.
// Call this after srv.Shutdown() in main.go so in-flight messages are not lost.
func (ctr *Controller) Stop() {
	logrus.Warn("[SDP] Shutting down — draining in-flight messages...")
	ctr.worker.Stop()
	ctr.publisher.Close()
	logrus.Info("[SDP] ✅ Clean shutdown complete")
}
