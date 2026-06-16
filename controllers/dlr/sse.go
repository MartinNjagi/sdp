package dlr

import (
	"context"
	"encoding/json"

	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

// RedisSSENotifier implements the SSENotifier interface with a single
// Redis PUBLISH. The Node BFF already subscribes to this channel and
// routes every message straight to connected WebSocket/SSE clients — no
// additional plumbing is required on the SDP side. As long as the event
// lands on the channel, it gets routed; campaign progress events use the
// same channel for the same reason.
type RedisSSENotifier struct {
	rdc     *redis.Client
	channel string
}

// NewRedisSSENotifier constructs the notifier. channel should match
// cfg.SSEChannel (env SSE_REDIS_CHANNEL, default "ws:messages") so DLR
// updates and campaign progress events share a single subscriber loop on
// the Node BFF side.
func NewRedisSSENotifier(rdc *redis.Client, channel string) *RedisSSENotifier {
	return &RedisSSENotifier{rdc: rdc, channel: channel}
}

// Broadcast publishes the event as JSON to the shared channel.
// Satisfies dlr.SSENotifier. Errors are logged, not returned — a failed
// dashboard push must never block or fail the DLR reconciliation itself,
// since the DB update (the source of truth) has already succeeded by the
// time Broadcast is called.
func (n *RedisSSENotifier) Broadcast(event string, payload any) {
	body, err := json.Marshal(map[string]any{
		"event":   event,
		"payload": payload,
	})
	if err != nil {
		logrus.Errorf("[SSE] Marshal event=%s: %v", event, err)
		return
	}

	if err := n.rdc.Publish(context.Background(), n.channel, body).Err(); err != nil {
		logrus.Errorf("[SSE] Publish event=%s channel=%s: %v", event, n.channel, err)
	}
}
