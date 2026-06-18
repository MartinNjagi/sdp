package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
//
// One DispatchWorker instance is created per queue (VIP, Standard) by
// worker.go, each bound to a different queue name and given an independent
// pool size — VIP always gets more consumers than Standard regardless of
// how deep either queue grows. DispatchWorker never consumes the legacy
// "sms.outbound.q"; the queue it binds to is passed in explicitly by the
// caller and is always one of publisher.QueueVIP / publisher.QueueStandard.
type DispatchWorker struct {
	ctx        context.Context
	queueName  string
	ch         *amqplib.Channel
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

// newDispatchWorker constructs a DispatchWorker bound to a specific queue,
// with its own dedicated AMQP channel so a slow consumer on one queue never
// blocks the other. deps bundles every shared dependency so this signature
// stays short regardless of how many collaborators DispatchWorker needs.
func newDispatchWorker(
	ctx context.Context,
	conn *amqplib.Connection,
	queueName string,
	deps Deps,
	poolSize int,
) (*DispatchWorker, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("dispatch worker [%s]: open channel: %w", queueName, err)
	}

	// Prefetch 50 unacknowledged messages per channel — bounds memory and
	// prevents one slow MNO from starving the rest of the pool.
	if err := ch.Qos(50, 0, false); err != nil {
		_ = ch.Close()
		return nil, fmt.Errorf("dispatch worker [%s]: set QoS: %w", queueName, err)
	}

	return &DispatchWorker{
		ctx:       ctx,
		queueName: queueName,
		ch:        ch,
		pub:       deps.Publisher,
		router:    deps.Router,
		limiter:   deps.Limiter,
		disp:      deps.Dispatcher,
		hotWallet: deps.HotWallet,
		flusher:   deps.Flusher,
		db:        deps.DB,
		poolSize:  poolSize,
	}, nil
}

// start spawns poolSize consumer goroutines, each registering its own
// consumer tag so the broker round-robins deliveries across the pool.
func (w *DispatchWorker) start(wg *sync.WaitGroup) {
	for i := range w.poolSize {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			w.consume(id)
		}(i)
	}
	logrus.Infof("[DispatchWorker/%s] Pool of %d goroutines started", w.queueName, w.poolSize)
}

// stop closes the channel, which cancels all pending deliveries and causes
// each consume() loop to exit cleanly.
func (w *DispatchWorker) stop() {
	if w.ch != nil {
		_ = w.ch.Close()
	}
}

func (w *DispatchWorker) consume(id int) {
	tag := fmt.Sprintf("dispatch-%s-%d", w.queueName, id)
	deliveries, err := w.ch.Consume(
		w.queueName, // explicit queue name passed in by the caller — VIP or Standard
		tag,
		false, // autoAck=false — we ACK manually after the full pipeline succeeds
		false, // exclusive
		false, // noLocal
		false, // noWait
		nil,
	)
	if err != nil {
		logrus.Errorf("[DispatchWorker/%s-%d] Start consume: %v", w.queueName, id, err)
		return
	}

	logrus.Infof("[DispatchWorker/%s-%d] Listening", w.queueName, id)

	for {
		select {
		case <-w.ctx.Done():
			return
		case d, ok := <-deliveries:
			if !ok {
				// Channel closed — broker disconnected or stop() was called.
				return
			}
			w.handle(id, d)
		}
	}
}

// handle runs the full dispatch pipeline for one DispatchEnvelope:
// decode → wallet deduct → route → rate-limit → dispatch → DB update → ACK.
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
	// env.Cost is in integer message credits, not currency.
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
// dead-letters permanently — refunding the wallet in the latter case since
// the deduction already happened in step 2 of handle().
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

// refund credits the client back when a deduction already happened but the
// message ultimately could not be sent. amount is in integer credits;
// HotWallet.Refund's signature is float64 to match the dlr.WalletRefunder
// interface, so the cast happens once, here, at the boundary.
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
