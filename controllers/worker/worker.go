package worker

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/redis/go-redis/v9"
	"sdp/connections"
	"sdp/controllers/dispatcher"
	"sdp/controllers/mno_router"
	"sdp/controllers/publisher"
	"sdp/controllers/ratelimiter"
	"sdp/controllers/storage"
	"sdp/controllers/wallet"
	"sdp/data"
	"strings"
	"time"

	"sync"

	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

// Deps bundles every dependency Worker needs to construct its sub-pools.
// Collecting them into a struct keeps New's signature short and makes it
// easy to add a new dependency later without touching every call site.
type Deps struct {
	RMQManager   *connections.RMQManager
	Publisher    *publisher.Publisher
	RDC          *redis.Client
	Router       *mno_router.Router
	Limiter      *ratelimiter.Limiter
	Dispatcher   dispatcher.Dispatcher
	HotWallet    *wallet.HotWallet
	Flusher      *wallet.Flusher
	LedgerRefund *wallet.LedgerRefunder
	CostEngine   *data.CostEngine
	DB           *gorm.DB
	S3           *storage.S3Service
	S3Bucket     string
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
	rdc      *redis.Client
	db       *gorm.DB
}

// New constructs the Worker and all three sub-pools from a single Deps
// bundle plus ctx/cfg. Called once by SDP.New.
func New(ctx context.Context, cfg *data.AppConfig, deps Deps) (*Worker, error) {

	logrus.Printf("Workers Bulk %d, VIP %d, Standard %d, Flusher %d", cfg.WorkerPoolBulk, cfg.WorkerPoolVIP, cfg.WorkerPoolStandard, cfg.WalletFlushInterval)

	bulk, err := newBulkWorker(ctx, deps.RMQManager, deps.Publisher, deps.RDC, deps.Router, deps.CostEngine, deps.DB, deps.S3, cfg.WorkerPoolBulk, cfg.S3Bucket)
	if err != nil {
		return nil, err
	}

	vip, err := newDispatchWorker(ctx, deps.RMQManager, publisher.QueueVIP, deps, cfg.WorkerPoolVIP)
	if err != nil {
		return nil, err
	}

	standard, err := newDispatchWorker(ctx, deps.RMQManager, publisher.QueueStandard, deps, cfg.WorkerPoolStandard)
	if err != nil {
		return nil, err
	}

	return &Worker{
		bulk:     bulk,
		vip:      vip,
		standard: standard,
		flusher:  deps.Flusher,
		rdc:      deps.RDC,
		db:       deps.DB,
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

	// Start the background SENT batcher
	w.StartSentBatcher(ctx)

	logrus.Info("[Worker] All sub-pools running ✅ (bulk, vip, standard)")
}

// Stop closes every sub-pool's AMQP channel and waits for in-flight
// deliveries to be ACK'd or NACK'd before returning.
func (w *Worker) Stop() {
	// 1. Cancel all consumers (stops new messages)
	w.bulk.cancelConsumer()
	w.vip.cancelConsumer()
	w.standard.cancelConsumer()

	// 2. Wait for active workers to finish processing and ACK
	w.wg.Wait()
	logrus.Info("[Worker] All goroutines drained ✅")

	// 3. Safe to close channels now
	w.bulk.closeChannel()
	w.vip.closeChannel()
	w.standard.closeChannel()
}

// NormalizeMSISDN removes whitespace and strips the leading '+'
func NormalizeMSISDN(number string) string {
	number = strings.ReplaceAll(number, " ", "")
	return strings.TrimPrefix(number, "+")
}

// --------------------------------------------------------------------------
// SENT Status Batcher
// --------------------------------------------------------------------------

func (w *Worker) StartSentBatcher(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second) // Flush every 2 seconds
	go func() {
		logrus.Info("[Sent Batcher] Started background loop ✅")
		for {
			select {
			case <-ctx.Done():
				w.flushSentBatch(context.Background())
				return
			case <-ticker.C:
				w.flushSentBatch(ctx)
			}
		}
	}()
}

func (w *Worker) flushSentBatch(ctx context.Context) {
	// Pop up to 2000 SENT updates from Redis
	results, err := w.rdc.LPopCount(ctx, "dispatch:sent_updates", 2000).Result()
	if errors.Is(err, redis.Nil) || len(results) == 0 {
		return
	} else if err != nil {
		logrus.Errorf("[Sent Batcher] Redis read error: %v", err)
		return
	}

	type updatePayload struct {
		OutboxID      uint64 `json:"outbox_id"`
		ProviderMsgID string `json:"provider_msg_id"`
	}

	// Deduplicate (Just in case the same outbox ID was somehow pushed twice)
	updateMap := make(map[uint64]string)
	for _, res := range results {
		var p updatePayload
		_ = json.Unmarshal([]byte(res), &p)
		updateMap[p.OutboxID] = p.ProviderMsgID
	}

	// Bulk update using a single transaction
	err = w.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for id, msgID := range updateMap {
			if err := tx.Table("outboxes").
				Where("id = ?", id).
				Updates(map[string]interface{}{
					"status":     "SENT",
					"message_id": msgID,
					"updated_at": time.Now(),
				}).Error; err != nil {
				return err // Rollback everything if one fails
			}
		}
		return nil
	})

	if err != nil {
		logrus.Errorf("[Sent Batcher] Transaction update failed: %v", err)
		// Push the records back to Redis so we don't lose them!
		w.rdc.RPush(ctx, "dispatch:sent_updates", results)
	} else {
		logrus.Infof("[Sent Batcher] Successfully bulk-updated %d messages to SENT", len(updateMap))
	}
}
