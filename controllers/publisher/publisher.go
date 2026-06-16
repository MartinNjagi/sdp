package publisher

import (
	"context"
	"encoding/json"
	"fmt"
	"sdp/data"
	"time"

	amqplib "github.com/rabbitmq/amqp091-go"
	"github.com/sirupsen/logrus"
)

// Exchange and queue name constants — single source of truth used by both
// the publisher (declare + publish) and the workers (consume).
const (
	OutboundExchange = "sms.outbound"

	QueueVIP      = "sms.q.transactional.vip"      // OTPs, auth codes — max priority
	QueueStandard = "sms.q.transactional.standard" // Receipts, notifications
	QueueBulk     = "sms.q.bulk.campaigns"         // Campaign fan-out envelopes

	RoutingKeyVIP      = "transactional.vip"
	RoutingKeyStandard = "transactional.standard"
	RoutingKeyBulk     = "bulk.campaigns"

	// DeadLetterExchange — receives messages that exhaust all retries.
	DeadLetterExchange = "sms.dead"
	DeadLetterQueue    = "sms.dead.q"

	// messageTTL — 24 hours in milliseconds.
	messageTTL = 86_400_000
)

// Publisher owns a single AMQP channel, declares the full three-queue
// topology on construction, and exposes typed publish methods.
type Publisher struct {
	ch *amqplib.Channel
}

// New opens a channel, declares all exchanges and queues, and returns a
// ready Publisher.
func New(conn *amqplib.Connection) (*Publisher, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("publisher: open channel: %w", err)
	}

	if err := declareTopology(ch); err != nil {
		_ = ch.Close()
		return nil, err
	}

	logrus.Info("[Publisher] Three-queue topology ready ✅")
	return &Publisher{ch: ch}, nil
}

// --------------------------------------------------------------------------
// Typed publish methods
// --------------------------------------------------------------------------

// PublishVIP enqueues a TransactionalEnvelope on the VIP queue.
// Use for OTPs, auth codes, password resets — anything time-critical.
func (p *Publisher) PublishVIP(ctx context.Context, env data.TransactionalEnvelope) error {
	env.Priority = "high"
	return p.publish(ctx, RoutingKeyVIP, env)
}

// PublishTransactional enqueues a TransactionalEnvelope on the standard queue.
// Use for receipts, notifications, and non-urgent single messages.
func (p *Publisher) PublishTransactional(ctx context.Context, env data.TransactionalEnvelope) error {
	env.Priority = "normal"
	return p.publish(ctx, RoutingKeyStandard, env)
}

// PublishBulk enqueues a BulkEnvelope on the bulk queue.
// The BulkWorker will fan this out into N DispatchEnvelopes.
func (p *Publisher) PublishBulk(ctx context.Context, env data.BulkEnvelope) error {
	return p.publish(ctx, RoutingKeyBulk, env)
}

// PublishDispatch enqueues a fully compiled DispatchEnvelope.
// Called internally by BulkWorker and TransactionalWorker after compilation.
func (p *Publisher) PublishDispatch(ctx context.Context, env data.DispatchEnvelope) error {
	key := routingKeyForType(env.MessageType)
	return p.publish(ctx, key, env)
}

// --------------------------------------------------------------------------
// Internal
// --------------------------------------------------------------------------

func (p *Publisher) publish(ctx context.Context, routingKey string, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("publisher: marshal [%s]: %w", routingKey, err)
	}

	err = p.ch.PublishWithContext(
		ctx,
		OutboundExchange,
		routingKey,
		false,
		false,
		amqplib.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqplib.Persistent,
			Timestamp:    time.Now(),
			Body:         body,
		},
	)
	if err != nil {
		return fmt.Errorf("publisher: publish [%s]: %w", routingKey, err)
	}

	logrus.Debugf("[Publisher] → %s (%d bytes)", routingKey, len(body))
	return nil
}

// routingKeyForType maps a MessageType string to the correct routing key.
func routingKeyForType(messageType string) string {
	switch messageType {
	case "vip":
		return RoutingKeyVIP
	case "bulk":
		return RoutingKeyBulk
	default:
		return RoutingKeyStandard
	}
}

// Close releases the AMQP channel. Called by SDP.Stop().
func (p *Publisher) Close() {
	if p.ch != nil {
		_ = p.ch.Close()
	}
}

// --------------------------------------------------------------------------
// Topology declaration
// --------------------------------------------------------------------------

func declareTopology(ch *amqplib.Channel) error {
	// 1. Dead-letter exchange + queue.
	if err := ch.ExchangeDeclare(
		DeadLetterExchange, "direct", true, false, false, false, nil,
	); err != nil {
		return fmt.Errorf("publisher: declare DLX: %w", err)
	}
	if _, err := ch.QueueDeclare(
		DeadLetterQueue, true, false, false, false, nil,
	); err != nil {
		return fmt.Errorf("publisher: declare dead queue: %w", err)
	}
	if err := ch.QueueBind(DeadLetterQueue, "#", DeadLetterExchange, false, nil); err != nil {
		return fmt.Errorf("publisher: bind dead queue: %w", err)
	}

	// 2. Main outbound exchange.
	if err := ch.ExchangeDeclare(
		OutboundExchange, "direct", true, false, false, false, nil,
	); err != nil {
		return fmt.Errorf("publisher: declare outbound exchange: %w", err)
	}

	// 3. Declare and bind each queue.
	queues := []struct {
		name string
		key  string
	}{
		{QueueVIP, RoutingKeyVIP},
		{QueueStandard, RoutingKeyStandard},
		{QueueBulk, RoutingKeyBulk},
	}

	for _, q := range queues {
		args := amqplib.Table{
			"x-message-ttl":             int32(messageTTL),
			"x-dead-letter-exchange":    DeadLetterExchange,
			"x-dead-letter-routing-key": "dead." + q.key,
		}
		if _, err := ch.QueueDeclare(q.name, true, false, false, false, args); err != nil {
			return fmt.Errorf("publisher: declare queue %s: %w", q.name, err)
		}
		if err := ch.QueueBind(q.name, q.key, OutboundExchange, false, nil); err != nil {
			return fmt.Errorf("publisher: bind queue %s: %w", q.name, err)
		}
	}

	return nil
}
