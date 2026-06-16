package dlr

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

// Handler holds the Reconciler and exposes Gin-compatible handler methods.
// Registered as routes in router/app.go, one handler per MNO since each
// Telco uses a different payload shape.
type Handler struct {
	reconciler *Reconciler
}

// NewHandler constructs a Handler. The Reconciler is shared — the Handler
// is purely an HTTP-to-domain translator, it owns no state.
func NewHandler(rec *Reconciler) *Handler {
	return &Handler{reconciler: rec}
}

// --------------------------------------------------------------------------
// Africa's Talking DLR webhook
// --------------------------------------------------------------------------

// atDLRPayload is the form-encoded body Africa's Talking POSTs to our endpoint.
// AT sends DLRs as application/x-www-form-urlencoded, not JSON.
//
// Example incoming POST:
//
//	id=ATXid123&status=Success&phoneNumber=+254712345678&networkCode=63902
type atDLRPayload struct {
	ID          string `form:"id"`            // Provider message ID (matches Outbox.MessageID)
	Status      string `form:"status"`        // "Success", "Failed", etc.
	PhoneNumber string `form:"phoneNumber"`   // Destination MSISDN (for logging only)
	NetworkCode string `form:"networkCode"`   // MNO network code (for logging only)
	FailureCode string `form:"failureReason"` // Optional failure reason
}

// ATDeliveryReport handles POST /webhooks/dlr/at
// Africa's Talking calls this endpoint for every DLR event.
// We translate the AT payload to a RawDLR and hand it to the Reconciler.
func (h *Handler) ATDeliveryReport(c *gin.Context) {
	var payload atDLRPayload
	if err := c.ShouldBind(&payload); err != nil {
		logrus.Warnf("[DLR/AT] Malformed payload: %v", err)
		// Return 200 to AT regardless — if we return 4xx, AT will retry
		// indefinitely. We log and discard malformed payloads instead.
		c.JSON(http.StatusOK, gin.H{"message": "received"})
		return
	}

	log := logrus.WithFields(logrus.Fields{
		"provider_msg_id": payload.ID,
		"msisdn":          payload.PhoneNumber,
		"raw_status":      payload.Status,
		"network_code":    payload.NetworkCode,
	})

	if payload.ID == "" {
		log.Warn("[DLR/AT] Missing message ID — cannot reconcile")
		c.JSON(http.StatusOK, gin.H{"message": "received"})
		return
	}

	raw := RawDLR{
		ProviderMsgID: payload.ID,
		RawStatus:     payload.Status,
		Source:        "webhook_at",
	}

	if err := h.reconciler.Handle(c.Request.Context(), raw); err != nil {
		// Log the error but still return 200 to prevent AT from retrying.
		// The error is recoverable via dead-letter inspection or a re-sync job.
		log.Errorf("[DLR/AT] Reconcile error: %v", err)
		c.JSON(http.StatusOK, gin.H{"message": "received"})
		return
	}

	log.Infof("[DLR/AT] Reconciled ok")
	c.JSON(http.StatusOK, gin.H{"message": "received"})
}

// --------------------------------------------------------------------------
// Generic HTTP DLR webhook (for MNOs that use JSON)
// --------------------------------------------------------------------------

// genericDLRPayload is a fallback for MNOs that POST a simple JSON body.
// Map their fields to this struct by adjusting the json tags per-MNO.
type genericDLRPayload struct {
	MessageID string `json:"message_id"`
	Status    string `json:"status"`
}

// GenericDeliveryReport handles POST /webhooks/dlr/generic
// Useful during development or when integrating a new HTTP-based MNO
// that is not yet worth a dedicated handler.
func (h *Handler) GenericDeliveryReport(c *gin.Context) {
	var payload genericDLRPayload
	if err := c.ShouldBindJSON(&payload); err != nil {
		logrus.Warnf("[DLR/Generic] Malformed JSON: %v", err)
		c.JSON(http.StatusOK, gin.H{"message": "received"})
		return
	}

	if payload.MessageID == "" {
		logrus.Warn("[DLR/Generic] Missing message_id — discarding")
		c.JSON(http.StatusOK, gin.H{"message": "received"})
		return
	}

	raw := RawDLR{
		ProviderMsgID: payload.MessageID,
		RawStatus:     payload.Status,
		Source:        "webhook_generic",
	}

	if err := h.reconciler.Handle(c.Request.Context(), raw); err != nil {
		logrus.Errorf("[DLR/Generic] Reconcile error: %v", err)
	}

	c.JSON(http.StatusOK, gin.H{"message": "received"})
}
