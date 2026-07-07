package middleware

import (
	"net/http"
	"os"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

// AllowedMNOIPs Known MNO Subnets (Safaricom, Africa's Talking, etc.)
var AllowedMNOIPs = []string{
	"196.201.214.",  // Safaricom
	"196.201.213.",  // Safaricom
	"164.92.186.27", // Internal
	// Add Africa's Talking subnets here if they publish them
}

// WebhookGuard unifies URL Secret validation and IP Whitelisting for DLRs
func WebhookGuard() gin.HandlerFunc {
	return func(c *gin.Context) {
		// 1. Validate URL Secret to prevent endpoint discovery
		expectedSecret := os.Getenv("SDP_WEBHOOK_SECRET")
		if expectedSecret != "" && c.Param("secret") != expectedSecret {
			logrus.Warnf("[WebhookGuard] Invalid secret attempt from IP: %s", c.ClientIP())
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
			return
		}

		// 2. Validate IP Address (Bypass if local testing)
		env := os.Getenv("APP_ENV")
		if env == "local" || env == "development" {
			c.Next()
			return
		}

		clientIP := c.ClientIP()
		isAllowed := false

		for _, subnet := range AllowedMNOIPs {
			if strings.HasPrefix(clientIP, subnet) {
				isAllowed = true
				break
			}
		}

		if !isAllowed {
			logrus.Warnf("[WebhookGuard] Blocked request from non-MNO IP: %s", clientIP)
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "access denied"})
			return
		}

		c.Next()
	}
}
