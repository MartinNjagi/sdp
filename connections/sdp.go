package connections

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"io"
	"net/http"
	"sdp/data"
	"time"
)

type SDP struct {
	rdc *redis.Client
	cfg *data.AppConfig
}

const (
	// Keep the refresh token key hardcoded or move it to data.AppConfig alongside SDPTokenRedisKey
	redisKeyRefreshToken = "SAFARICOM:SDP:REFRESHTOKEN"
)

func (s *SDP) InitSDPToken(ctx context.Context, rdc *redis.Client, cfg *data.AppConfig) {
	s.rdc = rdc
	s.cfg = cfg

	go s.rdc.FlushDB(ctx)
}

// StartTokenRefresher launches the background goroutine that keeps the Safaricom
// bearer token fresh. It attempts to use the refresh token API first, falling
// back to a full username/password login if the refresh token expires.
func (s *SDP) StartTokenRefresher(ctx context.Context) {
	// 1. Initial boot-up check
	dataReturn, err := s.rdc.Get(ctx, s.cfg.SDPTokenRedisKey).Result()
	if err != nil || len(dataReturn) == 0 {
		logrus.Info("[Auth] No existing token found on startup, fetching new one...")
		s.getAccessToken(ctx) // Fetch synchronously so the app is ready immediately
	}

	// 2. Launch the background routine
	go func() {
		ticker := time.NewTicker(20 * time.Minute)
		defer ticker.Stop()

		logrus.Info("[Auth] Safaricom Token Refresher running (20m intervals) ✅")

		for {
			select {
			case <-ctx.Done():
				logrus.Info("[Auth] Stopping token refresher...")
				return

			case <-ticker.C:
				logrus.Debug("[Auth] Refreshing Safaricom Access Token...")

				// Try using the refresh token API first
				if err := s.getNewToken(ctx); err != nil {
					logrus.Warnf("[Auth] Refresh token failed: %v. Falling back to full login...", err)

					// Fall back to full username/password login
					if token := s.getAccessToken(ctx); token == "" {
						logrus.Error("[Auth] Critical: Failed to acquire new Safaricom token")
					}
				}
			}
		}
	}()
}

// getAccessToken performs a full login using username and password.
func (s *SDP) getAccessToken(ctx context.Context) string {
	apiUsername := s.cfg.SDPNewUsername
	apiPassword := s.cfg.SDPNewPassword
	tokenAPIURL := s.cfg.SDPNewTokenURL

	if tokenAPIURL == "" {
		tokenAPIURL = "https://dsdp-apinb.safaricom.com/api/auth/login"
	}

	payload := map[string]string{
		"username": apiUsername,
		"password": apiPassword,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		logrus.Errorf("[Auth] Marshal login payload: %v", err)
		return ""
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, tokenAPIURL, bytes.NewReader(body))
	if err != nil {
		logrus.Errorf("[Auth] Build login request: %v", err)
		return ""
	}

	req.Header.Set("X-Requested-With", "XMLHttpRequest")
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		logrus.Errorf("[Auth] Login HTTP request failed: %v", err)
		return ""
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode <= 202 {
		var result map[string]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			logrus.Errorf("[Auth] Decode login response: %v", err)
			return "" // Prevents panic on nil map
		}

		token, _ := result["token"].(string)
		refreshToken, _ := result["refreshToken"].(string)

		if token == "" || refreshToken == "" {
			logrus.Error("[Auth] Token or RefreshToken missing in API response")
			return ""
		}

		// Save refresh token
		if err := s.rdc.Set(ctx, redisKeyRefreshToken, refreshToken, 0).Err(); err != nil {
			logrus.Errorf("[Auth] Failed to save refresh token to Redis: %v", err)
		}

		// Save main token (Use s.cfg.SDPTokenRedisKey so the Dispatcher finds it!)
		if err := s.rdc.Set(ctx, s.cfg.SDPTokenRedisKey, token, 29*time.Minute).Err(); err != nil {
			logrus.Errorf("[Auth] Failed to save token to Redis: %v", err)
		}

		logrus.Info("[Auth] Successfully acquired full access token")
		return token
	}

	respBody, _ := io.ReadAll(resp.Body)
	logrus.Errorf("[Auth] Login failed with status %d: %s", resp.StatusCode, string(respBody))
	return ""
}

// getNewToken attempts to refresh the existing token using the saved refresh token.
func (s *SDP) getNewToken(ctx context.Context) error {
	refreshToken, err := s.rdc.Get(ctx, redisKeyRefreshToken).Result()
	if err != nil || refreshToken == "" {
		return fmt.Errorf("refresh token not found in redis")
	}

	refreshAPIURL := s.cfg.SDPRefreshTokenURL
	if refreshAPIURL == "" {
		refreshAPIURL = "https://dsvc.safaricom.com:9480/api/auth/RefreshToken"
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, refreshAPIURL, nil)
	if err != nil {
		return fmt.Errorf("build refresh request: %w", err)
	}

	req.Header.Set("X-Requested-With", "XMLHttpRequest")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Authorization", fmt.Sprintf("Bearer %s", refreshToken))

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("refresh HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode <= 202 {
		var result map[string]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return fmt.Errorf("decode refresh response: %w", err)
		}

		token, ok := result["token"].(string)
		if !ok || token == "" {
			return fmt.Errorf("token missing in refresh API response")
		}

		// Save main token (Use s.cfg.SDPTokenRedisKey so the Dispatcher finds it!)
		if err := s.rdc.SetEx(ctx, s.cfg.SDPTokenRedisKey, token, 29*time.Minute).Err(); err != nil {
			return fmt.Errorf("save refreshed token to Redis: %w", err)
		}

		logrus.Debug("[Auth] Successfully refreshed access token")
		return nil
	}

	respBody, _ := io.ReadAll(resp.Body)
	return fmt.Errorf("refresh API returned status %d: %s", resp.StatusCode, string(respBody))
}
