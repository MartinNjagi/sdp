package data

import "github.com/golang-jwt/jwt/v5"

type CustomClaims struct {
	UserID        uint   `json:"user_id"`
	Username      string `json:"username"`
	ClientID      uint   `json:"client_id"` // Crucial for Multi-Tenancy!
	PermissionIDs []uint `json:"permission_ids"`
	jwt.RegisteredClaims
}

type Permission struct {
	ID   uint   `json:"id"`
	Name string `json:"name"`
}

type AuditLogParams struct {
	UserID          uint        `json:"user_id,omitempty"`
	Username        string      `json:"username,omitempty"`
	Action          string      `json:"action,omitempty"`
	OldData         interface{} `json:"old_data,omitempty"`
	NewData         interface{} `json:"new_data,omitempty"`
	PerformedBy     *uint       `json:"performed_by,omitempty"`
	PerformedByName *string     `json:"performed_by_name,omitempty"`
	IPAddress       string      `json:"ip_address,omitempty"`
}

// IntrospectResponse is returned to the sdp service
type IntrospectResponse struct {
	Active      bool         `json:"active"`
	ClientID    uint         `json:"client_id,omitempty"`
	UserID      uint         `json:"user_id,omitempty"`
	Username    string       `json:"username,omitempty"`
	Permissions []Permission `json:"permissions,omitempty"`
	TokenType   string       `json:"token_type,omitempty"` // "jwt" or "api_key"
}
