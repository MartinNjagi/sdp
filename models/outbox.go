package models

import "time"

// Outbox It acts as the immutable ledger for ALL outgoing traffic (Bulk and Transactional).
type Outbox struct {
	ID       uint64 `gorm:"primaryKey;autoIncrement"`
	ClientID string `gorm:"index;size:50;not null"`

	// Nullable CampaignID: If null, it's a Transactional/Single SMS. If set, it's Bulk.
	// ADDED: uniqueIndex:idx_campaign_phone to prevent duplicate sends on worker crash
	CampaignID *uint64   `gorm:"index;uniqueIndex:idx_campaign_phone"`
	Campaign   *Campaign `gorm:"foreignKey:CampaignID;references:ID"`

	// We store both the relational ID and the raw string for the Outbox.
	// ADDED: uniqueIndex:idx_campaign_phone
	PhoneID     uint64      `gorm:"index;not null;uniqueIndex:idx_campaign_phone"`
	PhoneNumber PhoneNumber `gorm:"foreignKey:PhoneID;references:ID"`
	MSISDN      string      `gorm:"size:20;not null"`

	SenderID string `gorm:"size:20;not null"`
	Message  string `gorm:"type:text;not null"`

	// The MessageID returned by the SDP (e.g., Safaricom/Twilio) for DLR matching
	MessageID string `gorm:"uniqueIndex;size:100"`

	Status string  `gorm:"size:20;default:'PENDING'"`
	Cost   float64 `gorm:"default:0.0"`

	CreatedAt time.Time
	UpdatedAt time.Time
}

// Template represents the database model for SMS templates.
type Template struct {
	ID        uint   `gorm:"primaryKey"`
	ClientID  uint   `gorm:"index;not null"`
	Name      string `gorm:"type:varchar(100);not null"`
	Content   string `gorm:"type:text;not null"`
	Status    string `gorm:"type:varchar(20);default:'PENDING'"` // PENDING, APPROVED, REJECTED
	CreatedAt time.Time
	UpdatedAt time.Time
}
