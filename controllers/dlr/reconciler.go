package dlr

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm/clause"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
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
	db                *gorm.DB
	rdc               *redis.Client
	hotWalletRefunder WalletRefunder // Restores Redis balance
	ledgerRefunder    LedgerRefunder // Updates DB via HTTP (NEW)
	sseNotifier       SSENotifier
	webhookFirer      WebhookFirer
}

// Add the interface for the new LedgerRefunder
type LedgerRefunder interface {
	Refund(ctx context.Context, clientID string, amount float64, outboxID uint64) error
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
func NewReconciler(db *gorm.DB, rdc *redis.Client) *Reconciler {
	return &Reconciler{db: db, rdc: rdc}
}

// 1. The Lightning-Fast Webhook Handler
func (r *Reconciler) Handle(ctx context.Context, raw RawDLR) error {
	// Just dump the raw DLR into a Redis List and return immediately!
	dataBytes, err := json.Marshal(raw)
	if err != nil {
		return err
	}

	// Push to the right side of the list
	return r.rdc.RPush(ctx, "dlr:pending_updates", dataBytes).Err()
}

// WithWalletRefunder attaches the wallet service. Returns self for chaining.
func (r *Reconciler) WithWalletRefunder(w WalletRefunder) *Reconciler {
	r.hotWalletRefunder = w
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

// WithLedgerRefunder Add the setter
func (r *Reconciler) WithLedgerRefunder(l LedgerRefunder) *Reconciler {
	r.ledgerRefunder = l
	return r
}

// --------------------------------------------------------------------------
// Core reconciliation
// --------------------------------------------------------------------------

// outboxRecord is the minimal projection we need from the outboxes table.
type outboxRecord struct {
	ID        uint64
	ClientID  string
	MessageID string
	Status    string
	Cost      float64
}

// dispatchEffects runs wallet refund, SSE broadcast, and client webhook.
// Errors are logged but do not propagate — the DB update is the source of truth.
func (r *Reconciler) dispatchEffects(
	ctx context.Context,
	log *logrus.Entry,
	record outboxRecord,
	status string,
) {
	// Wallet refund — only on FAILED
	if status == StatusFailed {
		// 1. Hot Refund (Instant Redis credits back)
		if r.hotWalletRefunder != nil {
			err := r.hotWalletRefunder.Refund(ctx, record.ClientID, record.Cost, "DLR_FAILED")
			if err != nil {
				log.Errorf("dlr reconciler: hotWalletRefunder: failed to refund: %w", err)
			}
		}

		// 2. Cold Refund (HTTP to Wallet Service)
		if r.ledgerRefunder != nil {
			if err := r.ledgerRefunder.Refund(ctx, record.ClientID, record.Cost, record.ID); err != nil {
				log.Errorf("HTTP Ledger refund failed for client=%s amount=%.4f: %v", record.ClientID, record.Cost, err)
			} else {
				log.Infof("Ledger successfully refunded %.4f to client=%s", record.Cost, record.ClientID)
			}
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

// StartDLRFlusher runs in the background. Call this from sdp.Start()
func (r *Reconciler) StartDLRFlusher(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second) // Flush every 2 seconds

	go func() {
		logrus.Info("[DLR Flusher] Started batch processor ✅")
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				r.flushBatch(context.Background()) // Final flush on shutdown
				return
			case <-ticker.C:
				r.flushBatch(ctx)
			}
		}
	}()
}

func (r *Reconciler) flushBatch(ctx context.Context) {
	// Pop up to 1000 DLRs from Redis atomically
	// Note: LPOP count requires Redis 6.2+
	results, err := r.rdc.LPopCount(ctx, "dlr:pending_updates", 1000).Result()
	if errors.Is(err, redis.Nil) || len(results) == 0 {
		return // Nothing to process
	} else if err != nil {
		logrus.Errorf("[DLR Flusher] Redis read error: %v", err)
		return
	}

	// 1. Deduplicate incoming DLRs (MNOs sometimes send the same webhook twice instantly)
	// We only keep the latest status for each ProviderMsgID
	dlrMap := make(map[string]RawDLR)
	for _, res := range results {
		var raw RawDLR
		_ = json.Unmarshal([]byte(res), &raw)
		dlrMap[raw.ProviderMsgID] = raw
	}

	var msgIDs []string
	for id := range dlrMap {
		msgIDs = append(msgIDs, id)
	}

	// 2. Fetch all matching Outboxes in exactly ONE query
	var outboxes []outboxRecord
	if err := r.db.WithContext(ctx).
		Table("outboxes").
		Select("id, client_id, message_id, status, cost").
		Where("message_id IN ?", msgIDs).
		Find(&outboxes).Error; err != nil {
		logrus.Errorf("[DLR Flusher] DB Select error: %v", err)
		// Push them back to Redis so we don't lose them!
		r.rdc.RPush(ctx, "dlr:pending_updates", results)
		return
	}

	// 3. Prepare the Bulk Update and trigger side effects
	var toUpdate []map[string]interface{}

	for _, ob := range outboxes {
		if isTerminal(ob.Status) {
			continue // Already processed previously
		}

		rawDLR := dlrMap[ob.MessageID]
		newStatus := normalise(rawDLR.RawStatus)

		// Prepare map for GORM bulk update
		toUpdate = append(toUpdate, map[string]interface{}{
			"id":         ob.ID,
			"status":     newStatus,
			"updated_at": time.Now(),
		})

		// Trigger Refunds, Webhooks, SSE (These are asynchronous/fast)
		r.dispatchEffects(ctx, logrus.NewEntry(logrus.StandardLogger()), ob, newStatus)
	}

	if len(toUpdate) == 0 {
		return
	}

	// 4. Perform ONE massive Bulk Update!
	// This generates: INSERT INTO outboxes (id, status) VALUES (...) ON DUPLICATE KEY UPDATE status=VALUES(status)
	err = r.db.WithContext(ctx).Table("outboxes").
		Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "id"}}, // Match on Primary Key
			DoUpdates: clause.AssignmentColumns([]string{"status", "updated_at"}),
		}).
		Create(&toUpdate).Error

	if err != nil {
		logrus.Errorf("[DLR Flusher] Bulk Update failed: %v", err)
	} else {
		logrus.Infof("[DLR Flusher] Successfully bulk-updated %d message statuses", len(toUpdate))
	}
}
