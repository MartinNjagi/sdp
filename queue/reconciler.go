package queue

import (
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
	"strings"
	"time"
)

// RawDLR is the normalised input the Reconciler receives from any DLR source
// (SMPP Deliver_SM or HTTP webhook). The handler layer translates
// provider-specific payloads into this struct before calling Handle.
type RawDLR struct {
	// ProviderMsgID is the message ID the MNO returned on submission.
	// This is stored on the Outbox record as message_id and is the join key.
	ProviderMsgID string

	// RawStatus is the Telco-native status string (e.g. "DELIVRD", "000", "FAILED").
	RawStatus string

	// Source identifies which channel delivered this DLR — used for logging.
	// Examples: "smpp", "webhook_at", "webhook_twilio"
	Source string
}

// Platform-level status values written to the outboxes table.
const (
	StatusDelivered = "DELIVERED"
	StatusFailed    = "FAILED"
	StatusRejected  = "REJECTED"
)

// Reconciler updates the Outbox record for a received DLR and triggers
// downstream effects (wallet refund, SSE broadcast, client webhook).
// Additional service dependencies (wallet, SSE, webhook) are injected via
// setter methods so the struct can be constructed without them for unit tests.
type Reconciler struct {
	db *gorm.DB

	// Optional downstream hooks — nil-safe, skipped if not set.
	walletRefunder WalletRefunder
	sseNotifier    SSENotifier
	webhookFirer   WebhookFirer
}

// WalletRefunder is satisfied by your existing Wallet service.
type WalletRefunder interface {
	Refund(ctx context.Context, clientID string, amount float64, reason string) error
}

// SSENotifier pushes a live dashboard update.
type SSENotifier interface {
	Broadcast(event string, payload any)
}

// WebhookFirer delivers an HTTP POST to the client's configured webhook URL.
type WebhookFirer interface {
	Fire(ctx context.Context, clientID string, payload any) error
}

// NewReconciler creates a Reconciler with only the DB wired in.
// Use WithWalletRefunder / WithSSENotifier / WithWebhookFirer to attach
// optional downstream services after construction.
func NewReconciler(db *gorm.DB) *Reconciler {
	return &Reconciler{db: db}
}

// WithWalletRefunder attaches the wallet service. Returns self for chaining.
func (r *Reconciler) WithWalletRefunder(w WalletRefunder) *Reconciler {
	r.walletRefunder = w
	return r
}

// WithSSENotifier attaches the SSE broadcaster. Returns self for chaining.
func (r *Reconciler) WithSSENotifier(s SSENotifier) *Reconciler {
	r.sseNotifier = s
	return r
}

// WithWebhookFirer attaches the client webhook service. Returns self for chaining.
func (r *Reconciler) WithWebhookFirer(f WebhookFirer) *Reconciler {
	r.webhookFirer = f
	return r
}

// --------------------------------------------------------------------------
// Core reconciliation
// --------------------------------------------------------------------------

// outboxRecord is the minimal projection we need from the outboxes table.
type outboxRecord struct {
	ID       uint64
	ClientID string
	Status   string
	Cost     float64
}

// Handle is the main entry point. It:
//  1. Normalises the raw Telco status to a platform status.
//  2. Loads the matching Outbox record by provider message ID.
//  3. Updates the record's status.
//  4. Triggers refund / SSE / webhook based on the outcome.
func (r *Reconciler) Handle(ctx context.Context, raw RawDLR) error {
	log := logrus.WithFields(logrus.Fields{
		"source":          raw.Source,
		"provider_msg_id": raw.ProviderMsgID,
		"raw_status":      raw.RawStatus,
	})

	// 1. Normalise status.
	platformStatus := normalise(raw.RawStatus)
	log.Debugf("Normalised status → %s", platformStatus)

	// 2. Load the Outbox record.
	var record outboxRecord
	if err := r.db.WithContext(ctx).
		Table("outboxes").
		Select("id, client_id, status, cost").
		Where("message_id = ?", raw.ProviderMsgID).
		First(&record).Error; err != nil {
		return fmt.Errorf("dlr reconciler: lookup message_id=%s: %w", raw.ProviderMsgID, err)
	}

	// Idempotency guard: if the record is already in a terminal state, skip.
	if isTerminal(record.Status) {
		log.Infof("Outbox id=%d already in terminal state=%s — skipping", record.ID, record.Status)
		return nil
	}

	// 3. Update status in DB.

	if err := r.db.WithContext(ctx).
		Exec(`UPDATE outboxes SET status = ?, updated_at = ? WHERE id = ?`,
			platformStatus, time.Now(), record.ID).Error; err != nil {
		return fmt.Errorf("dlr reconciler: update outbox id=%d: %w", record.ID, err)
	}

	log.Infof("Outbox id=%d updated to %s", record.ID, platformStatus)

	// 4. Downstream effects — run best-effort; log errors but don't fail the
	//    reconciliation (the DB update already succeeded).
	r.dispatchEffects(ctx, log, record, platformStatus)

	return nil
}

// dispatchEffects runs wallet refund, SSE broadcast, and client webhook.
// Errors are logged but do not propagate — the DB update is the source of truth.
func (r *Reconciler) dispatchEffects(
	ctx context.Context,
	log *logrus.Entry,
	record outboxRecord,
	status string,
) {
	// Wallet refund — only on FAILED, and only if the service is wired in.
	if status == StatusFailed && r.walletRefunder != nil {
		if err := r.walletRefunder.Refund(ctx, record.ClientID, record.Cost, "DLR_FAILED"); err != nil {
			log.Errorf("Wallet refund failed for client=%s amount=%.4f: %v",
				record.ClientID, record.Cost, err)
		} else {
			log.Infof("Refunded %.4f to client=%s", record.Cost, record.ClientID)
		}
	}

	// SSE broadcast — update the live dashboard.
	if r.sseNotifier != nil {
		r.sseNotifier.Broadcast("dlr_update", map[string]any{
			"outbox_id": record.ID,
			"client_id": record.ClientID,
			"status":    status,
		})
	}

	// Client webhook — fire-and-forget POST to the client's configured URL.
	if r.webhookFirer != nil {
		if err := r.webhookFirer.Fire(ctx, record.ClientID, map[string]any{
			"outbox_id": record.ID,
			"status":    status,
		}); err != nil {
			log.Errorf("Client webhook failed for client=%s: %v", record.ClientID, err)
		}
	}
}

// --------------------------------------------------------------------------
// Status normalisation
// --------------------------------------------------------------------------

// normalise maps Telco-native status strings to platform statuses.
// The table covers Africa's Talking, common SMPP error codes, and
// generic HTTP provider conventions. Extend as new MNOs are added.
func normalise(raw string) string {
	switch strings.ToUpper(strings.TrimSpace(raw)) {
	// Africa's Talking
	case "SUCCESS", "DELIVRD", "DELIVERED":
		return StatusDelivered

	// SMPP standard
	case "000", "ENROUTE":
		return StatusDelivered

	// Rejections — invalid number, barred, out of coverage
	case "REJECTED", "REJECTD", "UNDELIV", "INVALID", "ABSENT", "EXPIRED":
		return StatusRejected

	// Failures
	case "FAILED", "DELETED", "UNKNOWN":
		return StatusFailed

	default:
		logrus.Warnf("dlr: unknown raw status %q — mapping to FAILED", raw)
		return StatusFailed
	}
}

// isTerminal returns true for statuses that should not be overwritten.
func isTerminal(status string) bool {
	switch status {
	case StatusDelivered, StatusFailed, StatusRejected:
		return true
	}
	return false
}
