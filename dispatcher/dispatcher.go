package dispatcher

import "context"

// Result is returned by every Dispatcher.Send call.
// The ProviderMsgID is stored on the Outbox record and used later
// to match incoming DLRs back to the correct message.
type Result struct {
	ProviderMsgID string // Telco-assigned message ID
}

// Dispatcher is the interface every MNO adapter must satisfy.
// New adapters (SMPP, Twilio, Africa's Talking, etc.) implement this and
// are injected into the Worker — the Worker never imports a concrete adapter.
type Dispatcher interface {
	// Send transmits a single message to the MNO.
	// Returns a Result on success, or an error the Worker uses to decide
	// whether to retry (temporary) or dead-letter (permanent).
	Send(ctx context.Context, msg Message) (*Result, error)

	// Name identifies this adapter in logs and metrics.
	// Examples: "http_at", "http_twilio", "smpp_safaricom"
	Name() string
}

// Message is the normalised payload the Worker hands to any Dispatcher.
// It contains everything needed to transmit the SMS — no DB access required
// inside the dispatcher layer.
type Message struct {
	OutboxID   uint64
	MSISDN     string
	SenderID   string
	Body       string
	CampaignID *uint64
}

// SendError wraps a dispatch failure with enough context for the Worker to
// classify it as retryable or permanent.
type SendError struct {
	Cause     error
	Retryable bool // true → requeue; false → dead-letter
}

func (e *SendError) Error() string { return e.Cause.Error() }
func (e *SendError) Unwrap() error { return e.Cause }

// Temporary returns a retryable SendError (e.g. throttling, network blip).
func Temporary(err error) *SendError { return &SendError{Cause: err, Retryable: true} }

// Permanent returns a non-retryable SendError (e.g. invalid number, blacklisted).
func Permanent(err error) *SendError { return &SendError{Cause: err, Retryable: false} }
