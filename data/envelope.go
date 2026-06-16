package data

// BulkEnvelope is the lean payload published by the Core Engine when a
// campaign is launched. It carries only the campaign metadata — the
// BulkWorker is responsible for fetching the contact list from S3/Minio,
// compiling each message, writing Outbox rows, and fan-out publishing
// individual DispatchEnvelopes.
type BulkEnvelope struct {
	CampaignID     uint64 `json:"campaign_id"`
	ClientID       string `json:"client_id"`
	TemplateID     string `json:"template_id"`
	SenderID       string `json:"sender_id"`
	ContactGroupID string `json:"contact_group_id"`
	FileURL        string `json:"file_url"` // S3/Minio presigned URL or object key
}

// TransactionalEnvelope is published by the Core Engine for single or
// API-triggered messages. The TransactionalWorker compiles the template,
// writes one Outbox row, then re-publishes as a DispatchEnvelope.
// If Priority is "high", the DispatchEnvelope goes to the VIP queue.
type TransactionalEnvelope struct {
	ClientID     string            `json:"client_id"`
	MSISDN       string            `json:"msisdn"`
	Template     string            `json:"template"`     // Template name or inline body
	Message      string            `json:"message"`      // Pre-compiled body (if no template)
	Replacements map[string]string `json:"replacements"` // {{name}} → "John" etc.
	SenderID     string            `json:"sender_id"`
	Priority     string            `json:"priority"` // "high" → VIP queue, "normal" → standard
	RetryCount   int               `json:"retry_count"`
}

// DispatchEnvelope is the terminal, atomic payload that the DispatchWorker
// consumes. It is fully compiled — no further DB reads, template resolution,
// or S3 fetches are needed. Both BulkWorker and TransactionalWorker produce
// this type after their respective reconstruction phases.
//
// This is the only envelope type the dispatcher layer ever sees.
type DispatchEnvelope struct {
	OutboxID    uint64  `json:"outbox_id"`
	ClientID    string  `json:"client_id"`
	MSISDN      string  `json:"msisdn"`
	SenderID    string  `json:"sender_id"`
	Message     string  `json:"message"`
	MessageType string  `json:"message_type"` // "vip" | "standard" | "bulk"
	Cost        float64 `json:"cost"`         // Per-message cost in KES, set by pricing engine
	RetryCount  int     `json:"retry_count"`
}

// DLREvent is published to the Redis Pub/Sub channel after every DLR
// reconciliation. The Node BFF SSE handler subscribes to this channel
// and forwards events to connected browser clients in real time.
type DLREvent struct {
	OutboxID   uint64  `json:"outbox_id"`
	ClientID   string  `json:"client_id"`
	MSISDN     string  `json:"msisdn"`
	Status     string  `json:"status"` // "DELIVERED" | "FAILED" | "REJECTED"
	Cost       float64 `json:"cost"`   // Echoed for dashboard debit display
	CampaignID *uint64 `json:"campaign_id,omitempty"`
	ProviderID string  `json:"provider_id"` // MNO-assigned message ID
	OccurredAt string  `json:"occurred_at"` // RFC3339
}

// WalletFlushPayload is the body sent to the Core Wallet Service during the
// batch flush. One entry per client that had deductions in the flush window.
type WalletFlushPayload struct {
	Entries []WalletFlushEntry `json:"entries"`
}

type WalletFlushEntry struct {
	ClientID     string  `json:"client_id"`
	Amount       float64 `json:"amount"` // Total deducted in the flush window (KES)
	MessageCount int     `json:"message_count"`
}

// DeductCreditRequest is the per-message deduction request passed from the
// DispatchWorker to the HotWallet before every Send call.
type DeductCreditRequest struct {
	ClientID   string  `json:"client_id"`
	Amount     float64 `json:"amount"`
	CampaignID *uint64 `json:"campaign_id,omitempty"`
}

// DeductCreditResult is returned by HotWallet.Deduct.
type DeductCreditResult struct {
	Success      bool    // false → insufficient funds
	BalanceAfter float64 // Remaining hot balance after deduction
}
