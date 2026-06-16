package models

import "time"

// ContactGroup allows clients to segment their address books
type ContactGroup struct {
	ID          uint64 `gorm:"primaryKey;autoIncrement"`
	ClientID    string `gorm:"uniqueIndex:idx_client_group_name;size:50;not null"`
	Name        string `gorm:"uniqueIndex:idx_client_group_name;size:255;not null"` // Prevents duplicate names per client
	Description string `gorm:"type:text"`
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// ContactGroupMember bridges the ContactGroup to the ClientAddressBook
type ContactGroupMember struct {
	GroupID  uint   `gorm:"primaryKey"`
	ClientID string `gorm:"primaryKey;size:50"` // Ensures strict tenant ownership
	PhoneID  uint64 `gorm:"primaryKey"`         // Links to the specific phone number
}
