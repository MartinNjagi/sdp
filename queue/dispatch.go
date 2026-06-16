package queue

import (
	"context"
	"fmt"
	amqplib "github.com/rabbitmq/amqp091-go"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
	"sdp/connections"
	"sdp/dispatcher"
	"sync"
	"time"
)

// Worker owns the AMQP consumer channel and spawns a configurable pool of
// goroutines that each run the full dispatch pipeline:
//
//	Dequeue → Route → Rate-limit → Dispatch → DB update → ACK/NACK
type Worker struct {
	ctx        context.Context
	cfg        *connections.Config
	ch         *amqplib.Channel
	router     *Router
	limiter    *Limiter
	dispatcher dispatcher.Dispatcher
	db         *gorm.DB
	wg         sync.WaitGroup
}

// New creates a Worker with its own dedicated AMQP channel.
// Using a separate channel from the Publisher means a slow consumer
// never blocks message ingestion.
func New(
	ctx context.Context,
	cfg *connections.Config,
	conn *amqplib.Connection,
	router *mno_router.Router,
	limiter *ratelimiter.Limiter,
	disp dispatcher.Dispatcher,
	db *gorm.DB,
) (*Worker, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("worker: open channel: %w", err)
	}

	// QoS — broker pushes at most prefetchCount messages before waiting for ACKs.
	if err := ch.Qos(prefetchCount, 0, false); err != nil {
		_ = ch.Close()
		return nil, fmt.Errorf("worker: set QoS: %w", err)
	}

	return &Worker{
		ctx:        ctx,
		cfg:        cfg,
		ch:         ch,
		router:     router,
		limiter:    limiter,
		dispatcher: disp,
		db:         db,
	}, nil
}

// Start spawns cfg.WorkerPoolSize consumer goroutines.
// Each goroutine registers its own consumer tag so the broker
// distributes messages across the pool via round-robin.
func (w *Worker) Start() {
	poolSize := w.cfg.WorkerPoolSize
	if poolSize <= 0 {
		poolSize = 5 // sensible default
	}

	for i := range poolSize {
		w.wg.Add(1)
		go func(id int) {
			defer w.wg.Done()
			w.consume(id)
		}(i)
	}

	logrus.Infof("[Worker] Pool of %d goroutines started", poolSize)
}

// Stop signals all goroutines to exit and waits for in-flight messages
// to be ACK'd or NACK'd before returning.
func (w *Worker) Stop() {
	// Closing the channel causes all pending Deliveries to be cancelled,
	// which unblocks the range loops in consume().
	if w.ch != nil {
		_ = w.ch.Close()
	}
	w.wg.Wait()
	logrus.Info("[Worker] All goroutines stopped cleanly")
}

// --------------------------------------------------------------------------
// Internal consume loop
// --------------------------------------------------------------------------

func (w *Worker) consume(id int) {
	tag := fmt.Sprintf("worker-%d", id)
	deliveries, err := w.ch.Consume(
		publisher.OutboundQueue,
		tag,
		false, // autoAck=false — we ACK manually after successful dispatch
		false, // exclusive
		false, // noLocal
		false, // noWait
		nil,
	)
	if err != nil {
		logrus.Errorf("[Worker-%d] Failed to start consume: %v", id, err)
		return
	}

	logrus.Infof("[Worker-%d] Listening on %s", id, publisher.OutboundQueue)

	for {
		select {
		case <-w.ctx.Done():
			logrus.Infof("[Worker-%d] Context cancelled — exiting", id)
			return

		case d, ok := <-deliveries:
			if !ok {
				// Channel closed — broker disconnected or Stop() was called.
				logrus.Infof("[Worker-%d] Delivery channel closed — exiting", id)
				return
			}
			w.handle(id, d)
		}
	}
}

// handle runs the full pipeline for a single delivery.
func (w *Worker) handle(workerID int, d amqplib.Delivery) {
	log := logrus.WithField("worker", workerID)

	// 1. Decode envelope.
	var env publisher.OutboundEnvelope
	if err := json.Unmarshal(d.Body, &env); err != nil {
		log.Errorf("Malformed envelope — dead-lettering: %v", err)
		// Unrecoverable: NACK without requeue → dead-letter exchange.
		_ = d.Nack(false, false)
		return
	}

	log = log.WithFields(logrus.Fields{
		"outbox_id": env.OutboxID,
		"msisdn":    env.MSISDN,
	})

	// 2. Resolve MNO route.
	route, err := w.router.Resolve(env.MSISDN)
	if err != nil {
		log.Errorf("No route — dead-lettering: %v", err)
		_ = d.Nack(false, false)
		w.markFailed(env.OutboxID, "NO_ROUTE")
		return
	}

	log = log.WithField("mno", route.Name)

	// 3. Apply TPS rate limit — blocks until a token is available or ctx cancels.
	if err := w.limiter.Wait(w.ctx, route.Name); err != nil {
		// Context cancelled during wait — requeue so another instance picks it up.
		log.Warnf("Rate limiter cancelled — requeuing: %v", err)
		_ = d.Nack(false, true)
		return
	}

	// 4. Dispatch.
	msg := dispatcher.Message{
		OutboxID:   env.OutboxID,
		MSISDN:     env.MSISDN,
		SenderID:   env.SenderID,
		Body:       env.Message,
		CampaignID: env.CampaignID,
	}

	result, dispErr := w.dispatcher.Send(w.ctx, msg)
	if dispErr != nil {
		w.handleDispatchError(log, d, env, dispErr)
		return
	}

	// 5. Mark SENT in DB with the provider message ID for DLR matching.
	if err := w.markSent(env.OutboxID, result.ProviderMsgID); err != nil {
		// DB write failed — requeue so we don't lose the message.
		// The dispatcher already sent it, so the worst case is a duplicate
		// DLR update, which is idempotent.
		log.Errorf("DB update failed after send — requeuing: %v", err)
		_ = d.Nack(false, true)
		return
	}

	// 6. ACK — message fully processed.
	_ = d.Ack(false)
	log.Debugf("Dispatched and ACK'd provider_msg_id=%s", result.ProviderMsgID)
}

func (w *Worker) handleDispatchError(
	log *logrus.Entry,
	d amqplib.Delivery,
	env publisher.OutboundEnvelope,
	err error,
) {
	var sendErr *dispatcher.SendError
	retryable := false

	// Check if the error is a SendError with retry guidance.
	if se, ok := err.(*dispatcher.SendError); ok {
		sendErr = se
		retryable = sendErr.Retryable
	}

	if retryable && env.RetryCount < maxRetries {
		log.Warnf("Temporary dispatch error (retry %d/%d): %v", env.RetryCount+1, maxRetries, err)

		// Re-publish with incremented retry count rather than NACK+requeue,
		// so we can add a backoff delay between attempts.
		env.RetryCount++
		time.Sleep(backoff(env.RetryCount))

		if pubErr := w.republish(env); pubErr != nil {
			log.Errorf("Republish failed — dead-lettering: %v", pubErr)
			_ = d.Nack(false, false)
			return
		}
		// ACK the original delivery — our republish is the retry.
		_ = d.Ack(false)
		return
	}

	// Permanent failure or retries exhausted.
	log.Errorf("Permanent dispatch error (retries=%d) — dead-lettering: %v", env.RetryCount, err)
	_ = d.Nack(false, false)
	w.markFailed(env.OutboxID, "DISPATCH_FAILED")
}

// republish re-enqueues an envelope with an updated retry count.
func (w *Worker) republish(env publisher.OutboundEnvelope) error {
	body, err := json.Marshal(env)
	if err != nil {
		return err
	}
	return w.ch.PublishWithContext(
		w.ctx,
		publisher.OutboundExchange,
		publisher.OutboundRoutingKey,
		false, false,
		amqplib.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqplib.Persistent,
			Timestamp:    time.Now(),
			Body:         body,
		},
	)
}

// --------------------------------------------------------------------------
// DB helpers
// --------------------------------------------------------------------------

func (w *Worker) markSent(outboxID uint64, providerMsgID string) error {
	return w.db.Exec(
		`UPDATE outboxes SET status = 'SENT', message_id = ?, updated_at = ? WHERE id = ?`,
		providerMsgID, time.Now(), outboxID,
	).Error
}

func (w *Worker) markFailed(outboxID uint64, reason string) {
	if err := w.db.Exec(
		`UPDATE outboxes SET status = 'FAILED', updated_at = ? WHERE id = ?`,
		time.Now(), outboxID,
	).Error; err != nil {
		logrus.Errorf("[Worker] markFailed outbox_id=%d reason=%s db_err=%v", outboxID, reason, err)
	}
}

// --------------------------------------------------------------------------
// Backoff
// --------------------------------------------------------------------------

// backoff returns an exponential delay capped at 30 s for retry attempt n.
func backoff(n int) time.Duration {
	d := time.Duration(1<<uint(n)) * time.Second // 2s, 4s, 8s, …
	if d > 30*time.Second {
		d = 30 * time.Second
	}
	return d
}
