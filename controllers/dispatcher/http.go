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

// atResponse is the top-level Africa's Talking Send SMS API response.
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

// HTTPDispatcher sends messages via Africa's Talking REST API.
// Swap the base URL and auth headers to adapt to any HTTP MNO.
type HTTPDispatcher struct {
	apiKey   string
	username string
	baseURL  string
	client   *http.Client
}

// NewHTTP constructs an HTTPDispatcher from config.
// Returns an error if required credentials are missing so the failure is
// caught at startup rather than at first dispatch.
func NewHTTP(cfg *data.AppConfig) (*HTTPDispatcher, error) {
	if cfg.ATAPIKey == "" || cfg.ATUsername == "" {
		return nil, fmt.Errorf("http dispatcher: AT_API_KEY and AT_USERNAME are required")
	}

	baseURL := cfg.ATBaseURL
	if baseURL == "" {
		baseURL = "https://api.africastalking.com/version1/messaging"
	}

	return &HTTPDispatcher{
		apiKey:   cfg.ATAPIKey,
		username: cfg.ATUsername,
		baseURL:  baseURL,
		client: &http.Client{
			Timeout: 15 * time.Second,
		},
	}, nil
}

// Name satisfies the Dispatcher interface.
func (d *HTTPDispatcher) Name() string { return "http_at" }

// Send transmits a single SMS via Africa's Talking and returns the provider
// message ID on success. Errors are wrapped as Temporary or Permanent so the
// Worker can classify them correctly.
func (d *HTTPDispatcher) Send(ctx context.Context, msg Message) (*Result, error) {
	log := logrus.WithFields(logrus.Fields{
		"dispatcher": d.Name(),
		"outbox_id":  msg.OutboxID,
		"msisdn":     msg.MSISDN,
	})

	form := url.Values{}
	form.Set("username", d.username)
	form.Set("to", msg.MSISDN)
	form.Set("message", msg.Body)
	form.Set("from", msg.SenderID)

	req, err := http.NewRequestWithContext(
		ctx, http.MethodPost, d.baseURL, strings.NewReader(form.Encode()),
	)
	if err != nil {
		return nil, Permanent(fmt.Errorf("build request: %w", err))
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("apiKey", d.apiKey)

	resp, err := d.client.Do(req)
	if err != nil {
		// Network-level error — safe to retry.
		return nil, Temporary(fmt.Errorf("http send: %w", err))
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, Temporary(fmt.Errorf("read response: %w", err))
	}

	// 5xx → temporary (provider-side problem, retry is safe)
	// 4xx → permanent (our payload is wrong, retrying won't help)
	if resp.StatusCode >= 500 {
		return nil, Temporary(fmt.Errorf("provider %d: %s", resp.StatusCode, body))
	}
	if resp.StatusCode >= 400 {
		return nil, Permanent(fmt.Errorf("provider %d: %s", resp.StatusCode, body))
	}

	var atResp atResponse
	if err := json.Unmarshal(body, &atResp); err != nil {
		return nil, Permanent(fmt.Errorf("parse response: %w", err))
	}

	recipients := atResp.SMSMessageData.Recipients
	if len(recipients) == 0 {
		return nil, Permanent(fmt.Errorf("no recipients in response: %s", body))
	}

	r := recipients[0]

	// AT returns non-200 status codes inside the JSON for individual recipients.
	// statusCode 100 = Success. Anything else is a delivery-layer rejection.
	if r.StatusCode != 100 {
		log.Warnf("AT rejection status=%s code=%d", r.Status, r.StatusCode)
		return nil, Permanent(fmt.Errorf("AT rejected: %s (code %d)", r.Status, r.StatusCode))
	}

	log.Debugf("Dispatched ok provider_msg_id=%s", r.MessageID)
	return &Result{ProviderMsgID: r.MessageID}, nil
}
