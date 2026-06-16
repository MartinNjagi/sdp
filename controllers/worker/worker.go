package worker

import (
	"context"
	"sdp/controllers/dispatcher"
	"sdp/controllers/mno_router"
	"sdp/controllers/publisher"
	"sdp/controllers/ratelimiter"
	"sdp/controllers/wallet"
	"sdp/data"

	"sync"

	amqplib "github.com/rabbitmq/amqp091-go"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

// Deps bundles every dependency Worker needs to construct its sub-pools.
// Collecting them into a struct keeps New's signature short and makes it
// easy to add a new dependency later without touching every call site.
type Deps struct {
	Conn       *amqplib.Connection
	Publisher  *publisher.Publisher
	Router     *mno_router.Router
	Limiter    *ratelimiter.Limiter
	Dispatcher dispatcher.Dispatcher
	HotWallet  *wallet.HotWallet
	Flusher    *wallet.Flusher
	CostEngine *data.CostEngine
	DB         *gorm.DB
}

// Worker is the orchestrator — it does NOT consume from RabbitMQ itself.
// It constructs and owns three independent sub-pools, each with its own
// dedicated AMQP channel and goroutine count:
//
//	BulkWorker                — fan-out expander (S3 → N DispatchEnvelopes)
//	                            consumes sms.q.bulk.campaigns
//	DispatchWorker (VIP)      — terminal sender, highest concurrency
//	                            consumes sms.q.transactional.vip
//	DispatchWorker (Standard) — terminal sender, medium concurrency
//	                            consumes sms.q.transactional.standard
//
// Pool sizes are independent (cfg.WorkerPoolVIP/Standard/Bulk) so VIP always
// has more consumers than bulk, regardless of campaign queue depth.
type Worker struct {
	bulk     *BulkWorker
	vip      *DispatchWorker
	standard *DispatchWorker
	flusher  *wallet.Flusher
	wg       sync.WaitGroup
}

// New constructs the Worker and all three sub-pools from a single Deps
// bundle plus ctx/cfg. Called once by SDP.New.
func New(ctx context.Context, cfg *data.AppConfig, deps Deps) (*Worker, error) {

	bulk, err := newBulkWorker(ctx, deps.Conn, deps.Publisher, deps.Router, deps.CostEngine, deps.DB, cfg.WorkerPoolBulk)
	if err != nil {
		return nil, err
	}

	vip, err := newDispatchWorker(ctx, deps.Conn, publisher.QueueVIP, deps, cfg.WorkerPoolVIP)
	if err != nil {
		return nil, err
	}

	standard, err := newDispatchWorker(ctx, deps.Conn, publisher.QueueStandard, deps, cfg.WorkerPoolStandard)
	if err != nil {
		return nil, err
	}

	return &Worker{
		bulk:     bulk,
		vip:      vip,
		standard: standard,
		flusher:  deps.Flusher,
	}, nil
}

// Start launches every sub-pool's goroutines. Called after the HTTP server
// is already accepting requests, so the DLR webhook route is live before
// any worker begins consuming.
func (w *Worker) Start(ctx context.Context) {
	w.bulk.start(&w.wg)
	w.vip.start(&w.wg)
	w.standard.start(&w.wg)

	// The Flusher runs its own goroutine and performs a final flush
	// internally on ctx cancellation, so it is not tracked by the WaitGroup.
	go w.flusher.Start(ctx)

	logrus.Info("[Worker] All sub-pools running ✅ (bulk, vip, standard)")
}

// Stop closes every sub-pool's AMQP channel and waits for in-flight
// deliveries to be ACK'd or NACK'd before returning.
func (w *Worker) Stop() {
	w.bulk.stop()
	w.vip.stop()
	w.standard.stop()
	w.wg.Wait()
	logrus.Info("[Worker] All goroutines drained ✅")
}
