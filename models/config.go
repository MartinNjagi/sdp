package models

import "time"

type AuditLog struct {
	ID              uint      `gorm:"column:id;primaryKey;autoIncrement"`
	UserID          uint      `gorm:"column:user_id;index;not null"` // ID of user being modified
	Username        string    `gorm:"column:username"`
	Action          string    `gorm:"column:action;not null"`    // e.g., "soft_delete", "update", "create"
	OldData         *string   `gorm:"column:old_data;type:json"` // JSON of old values
	NewData         *string   `gorm:"column:new_data;type:json"` // JSON of new values
	PerformedBy     *uint     `gorm:"column:performed_by"`       // ID of admin who performed action
	PerformedByName *string   `gorm:"column:performed_by_name"`
	IPAddress       *string   `gorm:"column:ip_address"`
	CreatedAt       time.Time `gorm:"column:created_at;autoCreateTime"`
}

func (AuditLog) TableName() string {
	return "audit_logs"
}
