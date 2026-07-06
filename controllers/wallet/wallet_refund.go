package wallet

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
)

// LedgerRefunder makes HMAC-signed HTTP calls to the Core Wallet Service
// to permanently record refunds in PostgreSQL.
type LedgerRefunder struct {
	walletSvcURL string
	httpClient   *http.Client
}

func NewLedgerRefunder(baseURL string, client *http.Client) *LedgerRefunder {
	return &LedgerRefunder{
		// Append the exact path your Wallet service expects
		walletSvcURL: baseURL + "/api/v1/internal/wallet/refund",
		httpClient:   client,
	}
}

// Refund makes the HTTP POST request.
func (r *LedgerRefunder) Refund(ctx context.Context, clientID string, amount float64, outboxID uint64) error {
	cID, _ := strconv.ParseUint(clientID, 10, 32)

	// payload matches the wallet's data.RefundCreditRequest
	payload := map[string]interface{}{
		"client_id":   uint(cID),
		"amount":      uint(amount),
		"campaign_id": fmt.Sprintf("%d", outboxID), // We use outbox ID as the unique campaign reference for idempotency
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal refund payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.walletSvcURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build refund request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("network error during ledger refund: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("wallet service rejected refund (HTTP %d)", resp.StatusCode)
	}

	return nil
}
