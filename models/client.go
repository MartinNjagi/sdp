package models

import "time"

// PhoneNumber represents the global, deduplicated list of MSISDNs.
type PhoneNumber struct {
	ID        uint64 `gorm:"primaryKey;autoIncrement"`
	MSISDN    string `gorm:"uniqueIndex;size:20;not null"` // e.g., 254700000000
	CreatedAt time.Time
	UpdatedAt time.Time
}

// ClientAddressBook manages the relationship between a client and a phone number.
// This enforces deduplication per client and handles per-client blacklisting.
type ClientAddressBook struct {
	ClientID      string      `gorm:"primaryKey;size:50"`
	PhoneID       uint64      `gorm:"primaryKey"`
	PhoneNumber   PhoneNumber `gorm:"foreignKey:PhoneID;references:ID"`
	ContactName   string      `gorm:"size:100"`
	IsBlacklisted bool        `gorm:"default:false;index"`
	CreatedAt     time.Time
	UpdatedAt     time.Time
}
