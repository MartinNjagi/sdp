package data

// OutboundEnvelope is the compact JSON payload published per Outbox record.
// Workers read this and need no DB round-trip to dispatch the message.
type OutboundEnvelope struct {
	OutboxID   uint64  `json:"outbox_id"`
	MSISDN     string  `json:"msisdn"`
	SenderID   string  `json:"sender_id"`
	Message    string  `json:"message"`
	CampaignID *uint64 `json:"campaign_id,omitempty"`
	RetryCount int     `json:"retry_count"`
}
