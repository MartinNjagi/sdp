package dispatcher

import (
	"context"
	"fmt"
	"sdp/data"
)

// SMPPDispatcher will hold a persistent SMPP transceiver bind.
// It is stubbed here so the interface is satisfied and the build compiles
// while the full SMPP binding (EnquireLink heartbeat, windowing, PDU parsing)
// is implemented in a subsequent phase.
type SMPPDispatcher struct {
	cfg *data.AppConfig
}

// NewSMPP constructs an SMPPDispatcher. The actual TCP bind to the SMSC
// and the EnquireLink keepalive goroutine will be established here once
// the SMPP phase begins.
func NewSMPP(cfg *data.AppConfig) (*SMPPDispatcher, error) {
	if cfg.SMPPHost == "" {
		return nil, fmt.Errorf("smpp dispatcher: SMPP_HOST is required")
	}
	return &SMPPDispatcher{cfg: cfg}, nil
}

// Name satisfies the Dispatcher interface.
func (d *SMPPDispatcher) Name() string { return "smpp" }

// Send is not yet implemented — returns a permanent error so the Worker
// dead-letters rather than retrying indefinitely.
func (d *SMPPDispatcher) Send(_ context.Context, msg Message) (*Result, error) {
	return nil, Permanent(fmt.Errorf(
		"smpp dispatcher not yet implemented for outbox_id=%d", msg.OutboxID,
	))
}
