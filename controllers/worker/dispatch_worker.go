package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"sdp/controllers/dispatcher"
	"sdp/controllers/mno_router"
	"sdp/controllers/publisher"
	"sdp/controllers/ratelimiter"
	"sdp/controllers/wallet"
	"sdp/data"
	"time"

	amqplib "github.com/rabbitmq/amqp091-go"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

// DispatchWorker is the terminal consumer — it takes fully compiled
// DispatchEnvelopes and runs the complete pipeline:
//
//	Dequeue → Wallet deduct → Route → Rate-limit → Dispatch → DB update → ACK
//
// One DispatchWorker instance is created per queue (VIP, Standard) and
// given a different pool size so VIP messages always have more consumers.
type DispatchWorker struct {
	ctx       context.Context
	queueName string
	ch        *amqplib.Channel
	pub       *publisher.Publisher
	router    *mno_router.Router
	limiter   *ratelimiter.Limiter
	disp      dispatcher.Dispatcher
	hotWallet *wallet.HotWallet
	flusher   *wallet.Flusher
	db        *gorm.DB
	poolSize  int
}

// newDispatchWorker constructs a DispatchWorker bound to a specific queue.
func newDispatchWorker(
	ctx context.Context,
	conn *amqplib.Connection,
	queueName string,
	pub *publisher.Publisher,
	router *mno_router.Router,
	limiter *ratelimiter.Limiter,
	disp dispatcher.Dispatcher,
	hotWallet *wallet.HotWallet,
	flusher *wallet.Flusher,
	db *gorm.DB,
	poolSize int,
) (*DispatchWorker, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("dispatch worker [%s]: open channel: %w", queueName, err)
	}

	if err := ch.Qos(50, 0, false); err != nil {
		_ = ch.Close()
		return nil, fmt.Errorf("dispatch worker [%s]: set QoS: %w", queueName, err)
	}

	return &DispatchWorker{
		ctx:       ctx,
		queueName: queueName,
		ch:        ch,
		pub:       pub,
		router:    router,
		limiter:   limiter,
		disp:      disp,
		hotWallet: hotWallet,
		flusher:   flusher,
		db:        db,
		poolSize:  poolSize,
	}, nil
}

func (w *DispatchWorker) start(wg interface {
	Add(int)
	Done()
}) {
	for i := range w.poolSize {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			w.consume(id)
		}(i)
	}
	logrus.Infof("[DispatchWorker/%s] Pool of %d goroutines started", w.queueName, w.poolSize)
}

func (w *DispatchWorker) stop() {
	if w.ch != nil {
		_ = w.ch.Close()
	}
}

func (w *DispatchWorker) consume(id int) {
	tag := fmt.Sprintf("dispatch-%s-%d", w.queueName, id)
	deliveries, err := w.ch.Consume(
		w.queueName, tag, false, false, false, false, nil,
	)
	if err != nil {
		logrus.Errorf("[DispatchWorker/%s-%d] Start consume: %v", w.queueName, id, err)
		return
	}

	for {
		select {
		case <-w.ctx.Done():
			return
		case d, ok := <-deliveries:
			if !ok {
				return
			}
			w.handle(id, d)
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
	if env.Cost > 0 {
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
			// Insufficient funds — dead-letter, do not retry.
			log.Warnf("Insufficient credits (balance=%d, cost=%d) — dead-lettering",
				result.BalanceAfter, env.Cost)
			_ = d.Nack(false, false)
			w.markFailed(env.OutboxID, "INSUFFICIENT_FUNDS")
			return
		}

		// Track this client for the next batch flush.
		w.flusher.TrackActiveClient(w.ctx, env.ClientID)
		log.Debugf("Deducted %d credits — balance_after=%d", env.Cost, result.BalanceAfter)
	}

	// 3. Resolve MNO route.
	route, err := w.router.Resolve(env.MSISDN)
	if err != nil {
		log.Errorf("No route — dead-lettering: %v", err)
		_ = d.Nack(false, false)
		w.markFailed(env.OutboxID, "NO_ROUTE")
		// Refund since we already deducted.
		w.refund(env.ClientID, env.Cost, "NO_ROUTE")
		return
	}

	log = log.WithField("mno", route.Name)

	// 4. Rate limit — block until a TPS slot is available.
	if err := w.limiter.Wait(w.ctx, route.Name); err != nil {
		log.Warnf("Rate limiter cancelled — requeuing: %v", err)
		_ = d.Nack(false, true)
		w.refund(env.ClientID, env.Cost, "RATE_LIMIT_CANCEL")
		return
	}

	// 5. Dispatch.
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

	// 6. Mark SENT — store provider message ID for DLR matching.
	if err := w.markSent(env.OutboxID, result.ProviderMsgID); err != nil {
		log.Errorf("DB update after send failed — requeuing: %v", err)
		_ = d.Nack(false, true)
		return
	}

	_ = d.Ack(false)
	log.Debugf("Dispatched ✅ provider_msg_id=%s", result.ProviderMsgID)
}

// handleDispatchError classifies the error and decides whether to retry or
// dead-letter. Refunds the wallet on permanent failure.
func (w *DispatchWorker) handleDispatchError(
	log *logrus.Entry,
	d amqplib.Delivery,
	env data.DispatchEnvelope,
	err error,
) {
	retryable := false
	if se, ok := err.(*dispatcher.SendError); ok {
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
