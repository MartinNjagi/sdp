package dispatcher

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sdp/data"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
)

// --------------------------------------------------------------------------
// Africa's Talking dispatcher
// --------------------------------------------------------------------------

type atResponse struct {
	SMSMessageData struct {
		Message    string `json:"Message"`
		Recipients []struct {
			StatusCode int    `json:"statusCode"`
			Number     string `json:"number"`
			Status     string `json:"status"`
			MessageID  string `json:"messageId"`
			Cost       string `json:"cost"`
		} `json:"Recipients"`
	} `json:"SMSMessageData"`
}

// ATDispatcher sends messages via Africa's Talking REST API.
type ATDispatcher struct {
	apiKey   string
	username string
	baseURL  string
	client   *http.Client
}

// NewHTTP constructs an ATDispatcher from config.
func NewHTTP(cfg *data.AppConfig) (*ATDispatcher, error) {
	if cfg.ATAPIKey == "" || cfg.ATUsername == "" {
		return nil, fmt.Errorf("http dispatcher: AT_API_KEY and AT_USERNAME are required")
	}
	baseURL := cfg.ATBaseURL
	if baseURL == "" {
		baseURL = "https://api.africastalking.com/version1/messaging"
	}
	return &ATDispatcher{
		apiKey:   cfg.ATAPIKey,
		username: cfg.ATUsername,
		baseURL:  baseURL,
		client:   &http.Client{Timeout: 15 * time.Second},
	}, nil
}

func (d *ATDispatcher) Name() string { return "http_at" }

func (d *ATDispatcher) Send(ctx context.Context, msg Message) (*Result, error) {
	form := url.Values{}
	form.Set("username", d.username)
	form.Set("to", msg.MSISDN)
	form.Set("message", msg.Body)
	form.Set("from", msg.SenderID)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.baseURL, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, Permanent(fmt.Errorf("AT: build request: %w", err))
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("apiKey", d.apiKey)

	resp, err := d.client.Do(req)
	if err != nil {
		return nil, Temporary(fmt.Errorf("AT: http send: %w", err))
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode >= 500 {
		return nil, Temporary(fmt.Errorf("AT: provider %d: %s", resp.StatusCode, body))
	}
	if resp.StatusCode >= 400 {
		return nil, Permanent(fmt.Errorf("AT: provider %d: %s", resp.StatusCode, body))
	}

	var atResp atResponse
	if err := json.Unmarshal(body, &atResp); err != nil {
		return nil, Permanent(fmt.Errorf("AT: parse response: %w", err))
	}
	if len(atResp.SMSMessageData.Recipients) == 0 {
		return nil, Permanent(fmt.Errorf("AT: no recipients in response"))
	}

	r := atResp.SMSMessageData.Recipients[0]
	if r.StatusCode != 100 {
		return nil, Permanent(fmt.Errorf("AT: rejected status=%s code=%d", r.Status, r.StatusCode))
	}

	return &Result{ProviderMsgID: r.MessageID}, nil
}

// --------------------------------------------------------------------------
// Safaricom SDP dispatcher
// --------------------------------------------------------------------------

// SafaricomDispatcher sends messages via the Safaricom SDP REST API.
// It supports two modes driven by Message.MessageType:
//   - "vip" / "standard" → POST /SDP/sendSMSRequest (shortcode/premium transactional)
//   - anything else (e.g. "bulk") → POST /CMS/bulksms (campaigns)
//
// The Bearer token is supplied by an injected tokenGetter closure that
// reads from Redis — auth refresh is someone else's job, this dispatcher
// only consumes the cached token. A fallback URL is attempted on
// network-level failures (status 0) for the bulk endpoint.
type SafaricomDispatcher struct {
	cfg         *data.AppConfig
	client      *http.Client
	tokenGetter func() string // injected — reads current Bearer token from Redis
}

// safaricomBulkResponse mirrors the Safaricom bulksms API response shape.
type safaricomBulkResponse struct {
	Keyword    string `json:"keyword"`
	Status     string `json:"status"`
	StatusCode string `json:"statusCode"`
}

// NewSafaricom constructs a SafaricomDispatcher.
// tokenGetter is a closure that returns the current Bearer token — keeps
// auth/refresh concerns entirely out of the dispatcher itself.
func NewSafaricom(cfg *data.AppConfig, tokenGetter func() string) (*SafaricomDispatcher, error) {
	if cfg.SDPCpID == "" {
		return nil, fmt.Errorf("safaricom dispatcher: SDP_CPID is required")
	}
	if tokenGetter == nil {
		return nil, fmt.Errorf("safaricom dispatcher: tokenGetter must not be nil")
	}
	return &SafaricomDispatcher{
		cfg:         cfg,
		client:      &http.Client{Timeout: 30 * time.Second},
		tokenGetter: tokenGetter,
	}, nil
}

func (d *SafaricomDispatcher) Name() string { return "http_safaricom" }

// Send routes to the correct Safaricom endpoint based on message type.
func (d *SafaricomDispatcher) Send(ctx context.Context, msg Message) (*Result, error) {
	switch msg.MessageType {
	case "vip", "standard":
		return d.sendTransactional(ctx, msg)
	default:
		return d.sendBulk(ctx, msg)
	}
}

// sendBulk hits the Safaricom CMS bulksms endpoint, with a fallback URL on
// network-level failure and 429/0-status retry classification.
func (d *SafaricomDispatcher) sendBulk(ctx context.Context, msg Message) (*Result, error) {
	log := logrus.WithFields(logrus.Fields{
		"dispatcher": d.Name(),
		"outbox_id":  msg.OutboxID,
		"msisdn":     msg.MSISDN,
		"mode":       "bulk",
	})

	bulkURL := d.cfg.SDPBulkSMSURL
	if bulkURL == "" {
		bulkURL = "https://dsdp-apinb.safaricom.com/api/public/CMS/bulksms"
	}
	fallbackURL := "https://dsvc.safaricom.com:9480/api/public/CMS/bulksms"

	dlrURL := d.cfg.SDPBulkDLRURL
	if dlrURL == "" {
		dlrURL = "https://reports-service.intouchvas.io/dlr/bulk/{outboxID}"
	}
	dlrURL = strings.ReplaceAll(dlrURL, "{outboxID}", fmt.Sprintf("%d", msg.OutboxID))

	payload := map[string]interface{}{
		"timeStamp": time.Now().UnixMilli(),
		"dataSet": []map[string]interface{}{
			{
				"userName":          d.cfg.SDPCpID,
				"channel":           coalesce(d.cfg.SDPBulkChannel, "sms"),
				"oa":                msg.SenderID,
				"msisdn":            msg.MSISDN,
				"message":           msg.Body,
				"uniqueId":          fmt.Sprintf("%d", msg.OutboxID),
				"actionResponseURL": dlrURL,
			},
		},
	}

	token := d.tokenGetter()
	headers := map[string]string{
		"X-Requested-With": "XMLHttpRequest",
		"Content-Type":     "application/json",
		"X-Country":        "KEN",
		"X-Authorization":  fmt.Sprintf("Bearer %s", token),
		"SourceAddress":    d.cfg.SDPSourceAddress,
	}

	body, status, err := d.postWithFallback(ctx, bulkURL, fallbackURL, payload, headers)
	if err != nil {
		return nil, Temporary(fmt.Errorf("safaricom bulk: %w", err))
	}

	if status == 429 {
		return nil, Temporary(fmt.Errorf("safaricom bulk: 429 too many requests"))
	}
	if status == 0 {
		return nil, Temporary(fmt.Errorf("safaricom bulk: no response from server"))
	}
	if status < 200 || status > 210 {
		return nil, Permanent(fmt.Errorf("safaricom bulk: unexpected status %d: %s", status, body))
	}

	var resp safaricomBulkResponse
	if err := json.Unmarshal([]byte(body), &resp); err != nil {
		return nil, Permanent(fmt.Errorf("safaricom bulk: parse response: %w", err))
	}

	if resp.StatusCode != "SC0000" {
		log.Warnf("Safaricom rejection: status=%s code=%s", resp.Status, resp.StatusCode)
		return nil, Permanent(fmt.Errorf("safaricom bulk: rejected code=%s", resp.StatusCode))
	}

	// Safaricom bulk does not return a per-message ID in the submit response —
	// the DLR webhook carries the correlation via uniqueId/outboxID.
	providerID := fmt.Sprintf("SAF-BULK-%d", msg.OutboxID)
	log.Debugf("Bulk dispatched ok provider_id=%s", providerID)
	return &Result{ProviderMsgID: providerID}, nil
}

// sendTransactional hits the Safaricom SDP sendSMSRequest endpoint.
// Used for shortcode / premium transactional messages (VIP and Standard).
func (d *SafaricomDispatcher) sendTransactional(ctx context.Context, msg Message) (*Result, error) {
	log := logrus.WithFields(logrus.Fields{
		"dispatcher": d.Name(),
		"outbox_id":  msg.OutboxID,
		"msisdn":     msg.MSISDN,
		"mode":       "transactional",
	})

	sendURL := d.cfg.SDPSendSMSURL
	if sendURL == "" {
		sendURL = "https://dsvc.safaricom.com:8480/api/public/SDP/sendSMSRequest"
	}

	type dataParam struct {
		Name  string `json:"Name"`
		Value string `json:"Value"`
	}
	payload := map[string]interface{}{
		"RequestID":        fmt.Sprintf("%d", msg.OutboxID),
		"RequestTimeStamp": time.Now().UnixMilli(),
		"Channel":          coalesce(d.cfg.SDPSendSMSChannel, "sms"),
		"SourceAddress":    d.cfg.SDPSourceAddress,
		"Operation":        "SendSMS",
		"RequestParam": map[string]interface{}{
			"Data": []dataParam{
				{Name: "Msisdn", Value: msg.MSISDN},
				{Name: "Content", Value: msg.Body},
				{Name: "Language", Value: "1"},
				{Name: "CpId", Value: d.cfg.SDPCpID},
			},
		},
	}

	token := d.tokenGetter()
	headers := map[string]string{
		"X-Requested-With": "XMLHttpRequest",
		"Content-Type":     "application/json",
		"x-Authorization":  fmt.Sprintf("Bearer %s", token),
		"X-Country":        "KEN",
	}

	body, status, err := d.post(ctx, sendURL, payload, headers)
	if err != nil {
		return nil, Temporary(fmt.Errorf("safaricom transactional: %w", err))
	}

	if status < 200 || status > 210 {
		return nil, Permanent(fmt.Errorf("safaricom transactional: status %d: %s", status, body))
	}

	var resp struct {
		ResponseParam struct {
			StatusCode  string `json:"StatusCode"`
			Description string `json:"Description"`
		} `json:"ResponseParam"`
	}
	if err := json.Unmarshal([]byte(body), &resp); err != nil {
		return nil, Permanent(fmt.Errorf("safaricom transactional: parse response: %w", err))
	}

	providerID := fmt.Sprintf("SAF-TX-%d", msg.OutboxID)
	log.Debugf("Transactional dispatched ok status_code=%s", resp.ResponseParam.StatusCode)
	return &Result{ProviderMsgID: providerID}, nil
}

// --------------------------------------------------------------------------
// HTTP helpers
// --------------------------------------------------------------------------

func (d *SafaricomDispatcher) post(
	ctx context.Context,
	targetURL string,
	payload interface{},
	headers map[string]string,
) (body string, status int, err error) {
	b, err := json.Marshal(payload)
	if err != nil {
		return "", 0, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, targetURL, strings.NewReader(string(b)))
	if err != nil {
		return "", 0, err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return "", 0, err
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	return string(respBody), resp.StatusCode, nil
}

func (d *SafaricomDispatcher) postWithFallback(
	ctx context.Context,
	primaryURL, fallbackURL string,
	payload interface{},
	headers map[string]string,
) (body string, status int, err error) {
	body, status, err = d.post(ctx, primaryURL, payload, headers)
	if err != nil || status == 0 {
		logrus.Warnf("[SafaricomDispatcher] Primary URL failed (%v) — trying fallback", err)
		body, status, err = d.post(ctx, fallbackURL, payload, headers)
	}
	return
}

func coalesce(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}
