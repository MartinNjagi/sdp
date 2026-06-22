package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sdp/connections"
	"sdp/controllers/dispatcher"
	"sdp/controllers/mno_router"
	"sdp/controllers/publisher"
	"sdp/controllers/ratelimiter"
	"sdp/controllers/wallet"
	"sdp/data"

	"sync"
	"time"

	amqplib "github.com/rabbitmq/amqp091-go"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

// maxRetries is the number of times a DispatchEnvelope is republished after
// a temporary dispatch error before being permanently dead-lettered.
const maxRetries = 3

// DispatchWorker is the terminal consumer — it takes fully compiled
// DispatchEnvelopes and runs the complete pipeline:
//
//	Dequeue → Wallet deduct → Route → Rate-limit → Dispatch → DB update → ACK
type DispatchWorker struct {
	ctx        context.Context
	queueName  string
	RMQManager *connections.RMQManager // Replaces raw connection
	ch         *amqplib.Channel        // Restored: Needs to hold the active channel
	pub        *publisher.Publisher
	router     *mno_router.Router
	limiter    *ratelimiter.Limiter
	disp       dispatcher.Dispatcher
	hotWallet  *wallet.HotWallet
	flusher    *wallet.Flusher
	db         *gorm.DB
	costEngine *data.CostEngine
	poolSize   int
}

// newDispatchWorker constructs a DispatchWorker bound to a specific queue.
func newDispatchWorker(
	ctx context.Context,
	rmqManager *connections.RMQManager, // Updated to take RMQManager
	queueName string,
	deps Deps,
	poolSize int,
) (*DispatchWorker, error) {

	// FIX: Use the manager's connection to open the channel
	ch, err := rmqManager.Conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("dispatch worker [%s]: open channel: %w", queueName, err)
	}

	// Prefetch 50 unacknowledged messages per channel
	if err := ch.Qos(50, 0, false); err != nil {
		_ = ch.Close()
		return nil, fmt.Errorf("dispatch worker [%s]: set QoS: %w", queueName, err)
	}

	return &DispatchWorker{
		ctx:        ctx,
		queueName:  queueName,
		RMQManager: rmqManager, // Save manager for reconnects
		ch:         ch,         // Save the active channel
		pub:        deps.Publisher,
		router:     deps.Router,
		limiter:    deps.Limiter,
		disp:       deps.Dispatcher,
		hotWallet:  deps.HotWallet,
		flusher:    deps.Flusher,
		db:         deps.DB,
		poolSize:   poolSize,
	}, nil
}

// start spawns poolSize consumer goroutines, each registering its own
// consumer tag so the broker round-robins deliveries across the pool.
func (w *DispatchWorker) start(wg *sync.WaitGroup) {
	for i := 0; i < w.poolSize; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			w.consume(id)
		}(i)
	}
	logrus.Infof("[DispatchWorker/%s] Pool of %d goroutines started", w.queueName, w.poolSize)
}

// Phase 1: Tell RabbitMQ to stop sending new messages to this worker pool
func (w *DispatchWorker) cancelConsumer() {
	if w.ch != nil {
		// Loop through every worker ID in the pool and cancel its specific tag
		for i := 0; i < w.poolSize; i++ {
			tag := fmt.Sprintf("dispatch-%s-%d", w.queueName, i)

			// This stops new deliveries for this specific goroutine,
			// but keeps the channel OPEN so we can still ACK messages we already have.
			if err := w.ch.Cancel(tag, false); err != nil {
				logrus.Warnf("[DispatchWorker/%s] Failed to cancel consumer %s: %v", w.queueName, tag, err)
			} else {
				logrus.Infof("[DispatchWorker/%s] Cancelled consumer: %s", w.queueName, tag)
			}
		}
	}
}

// Phase 3: Close the channel completely
func (w *DispatchWorker) closeChannel() {
	if w.ch != nil {
		_ = w.ch.Close()
	}
}

func (w *DispatchWorker) consume(id int) {
	tag := fmt.Sprintf("dispatch-%s-%d", w.queueName, id)

	// FIX: Outer loop to handle auto-reconnection
	for {
		// 1. Ensure we have a valid, open channel
		if w.ch == nil || w.ch.IsClosed() {
			newCh, err := w.RMQManager.Conn.Channel()
			if err != nil {
				logrus.Errorf("[DispatchWorker/%s-%d] Failed to recreate channel: %v. Retrying...", w.queueName, id, err)
				time.Sleep(2 * time.Second)
				continue
			}

			// Re-apply QoS on the new channel
			_ = newCh.Qos(50, 0, false)
			w.ch = newCh
		}

		// 2. Start Consuming
		deliveries, err := w.ch.Consume(
			w.queueName, tag, false, false, false, false, nil,
		)
		if err != nil {
			logrus.Errorf("[DispatchWorker/%s-%d] Consume failed: %v", w.queueName, id, err)
			time.Sleep(2 * time.Second)
			continue
		}

		logrus.Infof("[DispatchWorker/%s-%d] Listening", w.queueName, id)

		// 3. Inner process loop
	processLoop:
		for {
			select {
			case <-w.ctx.Done():
				return // App is shutting down

			case d, ok := <-deliveries:
				if !ok {
					// Channel closed! Break out of the inner loop
					logrus.Warnf("[DispatchWorker/%s-%d] Channel closed! Waiting for reconnect...", w.queueName, id)
					w.ch = nil // Force recreation on next tick

					// Wait for the RMQManager to signal the connection is back
					<-w.RMQManager.Reconnect
					break processLoop
				}

				w.handle(id, d)
			}
		}
	}
}

// handle runs the full dispatch pipeline for one DispatchEnvelope.
func (w *DispatchWorker) handle(workerID int, d amqplib.Delivery) {
	log := logrus.WithFields(logrus.Fields{
		"queue":  w.queueName,
		"worker": workerID,
	})

	// 1. Decode.
	var env data.DispatchEnvelope
	if err := json.Unmarshal(d.Body, &env); err != nil {
		log.Errorf("Malformed DispatchEnvelope — dead-lettering: %v", err)
		_ = d.Nack(false, false)
		return
	}

	log = log.WithFields(logrus.Fields{
		"outbox_id": env.OutboxID,
		"msisdn":    env.MSISDN,
		"client_id": env.ClientID,
	})

	// 2. Hot wallet deduction — atomic, before any network call.
	// FIX: Only deduct if this is the FIRST attempt (RetryCount == 0).
	// If it's > 0, we already deducted the money on a previous attempt!
	if env.Cost > 0 && env.RetryCount == 0 {
		result, err := w.hotWallet.Deduct(w.ctx, data.DeductCreditRequest{
			ClientID: env.ClientID,
			Amount:   env.Cost,
		})
		if err != nil {
			log.Errorf("Wallet deduct error — requeuing: %v", err)
			_ = d.Nack(false, true)
			return
		}
		if !result.Success {
			// Insufficient credits — dead-letter, do not retry.
			log.Warnf("Insufficient credits (balance=%d, cost=%d) — dead-lettering",
				result.BalanceAfter, env.Cost)
			_ = d.Nack(false, false)
			w.markFailed(env.OutboxID, "INSUFFICIENT_FUNDS")
			return
		}

		// Track this client so the next batch flush picks up the deduction.
		w.flusher.TrackActiveClient(w.ctx, env.ClientID)
		log.Debugf("Deducted %d credits — balance_after=%d", env.Cost, result.BalanceAfter)
	}

	// 3. Resolve MNO route.
	route, err := w.router.Resolve(env.MSISDN)
	if err != nil {
		log.Errorf("No route — dead-lettering: %v", err)
		_ = d.Nack(false, false)
		w.markFailed(env.OutboxID, "NO_ROUTE")
		w.refund(env.ClientID, env.Cost, "NO_ROUTE") // already deducted — give it back
		return
	}

	log = log.WithField("mno", route.Name)

	// 4. Rate limit — block until a TPS slot is available, or ctx cancels.
	if err := w.limiter.Wait(w.ctx, route.Name); err != nil {
		log.Warnf("Rate limiter cancelled — requeuing: %v", err)
		_ = d.Nack(false, true)
		w.refund(env.ClientID, env.Cost, "RATE_LIMIT_CANCEL")
		return
	}

	// 5. Dispatch to the MNO.
	msg := dispatcher.Message{
		OutboxID:    env.OutboxID,
		MSISDN:      env.MSISDN,
		SenderID:    env.SenderID,
		Body:        env.Message,
		MessageType: env.MessageType,
	}

	result, dispErr := w.disp.Send(w.ctx, msg)
	if dispErr != nil {
		w.handleDispatchError(log, d, env, dispErr)
		return
	}

	// 6. Mark SENT — store the provider message ID for later DLR matching.
	if err := w.markSent(env.OutboxID, result.ProviderMsgID); err != nil {
		log.Errorf("DB update after send failed — requeuing: %v", err)
		_ = d.Nack(false, true)
		return
	}

	_ = d.Ack(false)
	log.Debugf("Dispatched ✅ provider_msg_id=%s", result.ProviderMsgID)
}

// handleDispatchError classifies the error via dispatcher.SendError and
// either republishes with backoff (temporary, retries remaining) or
// dead-letters permanently.
func (w *DispatchWorker) handleDispatchError(
	log *logrus.Entry,
	d amqplib.Delivery,
	env data.DispatchEnvelope,
	err error,
) {
	retryable := false
	var se *dispatcher.SendError
	if errors.As(err, &se) {
		retryable = se.Retryable
	}

	if retryable && env.RetryCount < maxRetries {
		env.RetryCount++
		delay := backoff(env.RetryCount)
		log.Warnf("Temporary error (retry %d/%d in %s): %v", env.RetryCount, maxRetries, delay, err)
		time.Sleep(delay)

		if pubErr := w.pub.PublishDispatch(w.ctx, env); pubErr != nil {
			log.Errorf("Republish failed — dead-lettering: %v", pubErr)
			_ = d.Nack(false, false)
			w.markFailed(env.OutboxID, "DISPATCH_FAILED")
			w.refund(env.ClientID, env.Cost, "DISPATCH_FAILED")
			return
		}
		// ACK the original delivery — the republish IS the retry.
		_ = d.Ack(false)
		return
	}

	log.Errorf("Permanent dispatch error (retries=%d) — dead-lettering: %v", env.RetryCount, err)
	_ = d.Nack(false, false)
	w.markFailed(env.OutboxID, "DISPATCH_FAILED")
	w.refund(env.ClientID, env.Cost, "DISPATCH_FAILED")
}

// --------------------------------------------------------------------------
// DB helpers
// --------------------------------------------------------------------------

func (w *DispatchWorker) markSent(outboxID uint64, providerMsgID string) error {
	return w.db.Exec(
		`UPDATE outboxes SET status = 'SENT', message_id = ?, updated_at = ? WHERE id = ?`,
		providerMsgID, time.Now(), outboxID,
	).Error
}

func (w *DispatchWorker) markFailed(outboxID uint64, reason string) {
	if err := w.db.Exec(
		`UPDATE outboxes SET status = 'FAILED', updated_at = ? WHERE id = ?`,
		time.Now(), outboxID,
	).Error; err != nil {
		logrus.Errorf("[DispatchWorker] markFailed outbox_id=%d reason=%s: %v", outboxID, reason, err)
	}
}

// --------------------------------------------------------------------------
// Wallet refund helper
// --------------------------------------------------------------------------

func (w *DispatchWorker) refund(clientID string, amount int64, reason string) {
	if amount <= 0 || clientID == "" {
		return
	}
	if err := w.hotWallet.Refund(context.Background(), clientID, float64(amount), reason); err != nil {
		logrus.Errorf("[DispatchWorker] Refund failed client=%s amount=%d reason=%s: %v",
			clientID, amount, reason, err)
	}
}

// --------------------------------------------------------------------------
// Backoff
// --------------------------------------------------------------------------

// backoff returns an exponential delay capped at 30s for retry attempt n.
func backoff(n int) time.Duration {
	d := time.Duration(1<<uint(n)) * time.Second // 2s, 4s, 8s, ...
	if d > 30*time.Second {
		d = 30 * time.Second
	}
	return d
}
