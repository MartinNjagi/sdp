package connections

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"github.com/sirupsen/logrus"
	"io"
	"net/http"
	"time"
)

// Transport automatically signs all outgoing HTTP requests with an HMAC signature.
type Transport struct {
	Secret string
	Base   http.RoundTripper // Allows overriding the default transport if needed
}

// RoundTrip executes a single HTTP transaction, adding the required HMAC headers.
func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	// 1. Clone the request so we don't modify the original (a Go transport rule)
	reqClone := req.Clone(req.Context())

	// 2. Read and restore the body for hashing
	var body []byte
	if reqClone.Body != nil {
		var err error
		body, err = io.ReadAll(reqClone.Body)
		if err != nil {
			return nil, fmt.Errorf("failed to read request body for signing: %w", err)
		}
		// Put it back for the actual HTTP transmission
		reqClone.Body = io.NopCloser(bytes.NewBuffer(body))
	}

	// 3. Generate Nonce and Timestamp
	ts := fmt.Sprintf("%d", time.Now().Unix())
	nonce, err := generateNonce()
	if err != nil {
		return nil, fmt.Errorf("failed to generate nonce: %w", err)
	}

	// ---> ADD THIS DEBUG LOG <---
	signaturePayload := fmt.Sprintf("%s.%s.%s", ts, nonce, string(body))
	logrus.Infof("HMAC CLIENT PAYLOAD TO SIGN:\n%s", signaturePayload)

	// 4. Compute HMAC
	mac := hmac.New(sha256.New, []byte(t.Secret))
	mac.Write([]byte(fmt.Sprintf("%s.%s.%s", ts, nonce, string(body))))
	signature := hex.EncodeToString(mac.Sum(nil))

	// 5. Attach Headers
	if reqClone.Header == nil {
		reqClone.Header = make(http.Header)
	}
	reqClone.Header.Set("X-Timestamp", ts)
	reqClone.Header.Set("X-Nonce", nonce)
	reqClone.Header.Set("X-Signature", signature)

	// 6. Proceed with the request using the underlying transport
	base := t.Base
	if base == nil {
		base = http.DefaultTransport
	}
	return base.RoundTrip(reqClone)
}

// generateNonce creates a 32-character secure random hex string
func generateNonce() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// NewHTTP creates a standard *http.Client pre-configured with the HMAC transport.
func NewHTTP(secret string, timeout time.Duration) *http.Client {
	return &http.Client{
		Transport: &Transport{Secret: secret},
		Timeout:   timeout,
	}
}
