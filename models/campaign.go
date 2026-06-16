package models

import "time"

type Campaign struct {
	ID           uint64 `gorm:"primaryKey;autoIncrement"`
	ClientID     string `gorm:"index;size:50;not null"`
	Name         string `gorm:"size:255;not null"`
	Status       string `gorm:"size:20;default:'PENDING'"` // PENDING, SCHEDULED, PROCESSING, COMPLETED, FAILED
	ScheduledFor *time.Time
	FileURL      string `gorm:"type:text"` // S3/GCS link for CSVs
	ContactGroup string `gorm:"size:100"`  // Target group ID from DB
	TemplateName string `gorm:"size:100"`  // Template to use
	SenderID     string `gorm:"size:50"`   // The MNO sender identity
	CreatedAt    time.Time
	UpdatedAt    time.Time
}
