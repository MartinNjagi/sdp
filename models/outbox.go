package models

import "time"

// Outbox It acts as the immutable ledger for ALL outgoing traffic (Bulk and Transactional).
type Outbox struct {
	ID       uint64 `gorm:"primaryKey;autoIncrement"`
	ClientID string `gorm:"index;size:50;not null"`

	// Nullable CampaignID: If null, it's a Transactional/Single SMS. If set, it's Bulk.
	CampaignID *uint64   `gorm:"index"`
	Campaign   *Campaign `gorm:"foreignKey:CampaignID;references:ID"`

	// We store both the relational ID and the raw string for the Outbox.
	// The string makes worker execution extremely fast (no joins needed to send).
	PhoneID     uint64      `gorm:"index;not null"`
	PhoneNumber PhoneNumber `gorm:"foreignKey:PhoneID;references:ID"`
	MSISDN      string      `gorm:"size:20;not null"`

	SenderID string `gorm:"size:20;not null"`
	Message  string `gorm:"type:text;not null"` // The final compiled text (templates replaced)

	// The MessageID returned by the SDP (e.g., Safaricom/Twilio) for DLR matching
	MessageID string `gorm:"uniqueIndex;size:100"`

	Status string  `gorm:"size:20;default:'PENDING'"` // PENDING, SENT, DELIVERED, FAILED
	Cost   float64 `gorm:"default:0.0"`               // Useful for billing per message part

	CreatedAt time.Time
	UpdatedAt time.Time
}
