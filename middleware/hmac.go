package middleware

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"io"
	"strconv"
	"time"
)

func VerifySignature(secret string, rdb *redis.Client) gin.HandlerFunc {
	return func(c *gin.Context) {
		// 1. Read timestamp header
		tsHeader := c.Request.Header.Get("X-Timestamp")
		ts, err := strconv.ParseInt(tsHeader, 10, 64)
		if err != nil {
			c.AbortWithStatusJSON(401, gin.H{"error": "missing or invalid timestamp"})
			return
		}

		// Read Nonce
		nonce := c.Request.Header.Get("X-Nonce")
		if nonce == "" {
			c.AbortWithStatusJSON(401, gin.H{"error": "missing nonce"})
			return
		}

		// 2. Reject if request is older than 30 seconds
		age := time.Now().Unix() - ts
		if age > 30 || age < -5 {
			c.AbortWithStatusJSON(401, gin.H{"error": "request timestamp expired"})
			return
		}

		// 3. Read and restore body
		body, _ := io.ReadAll(c.Request.Body)
		c.Request.Body = io.NopCloser(bytes.NewBuffer(body))

		// 4. Verify HMAC — now signs timestamp + nonce + body!
		mac := hmac.New(sha256.New, []byte(secret))

		// Format matches Node.js: timestamp + nonce + body
		mac.Write([]byte(fmt.Sprintf("%s.%s.%s", tsHeader, nonce, string(body))))

		expected := hex.EncodeToString(mac.Sum(nil))
		incoming := c.Request.Header.Get("X-Signature")

		if !hmac.Equal([]byte(incoming), []byte(expected)) {
			c.AbortWithStatusJSON(401, gin.H{"error": "invalid signature"})
			return
		}

		// 5. Nonce check — Use the signature as the key (since it's now guaranteed unique)
		nonceKey := fmt.Sprintf("nonce:sig:%s", incoming)
		set, err := rdb.SetNX(context.Background(), nonceKey, 1, 60*time.Second).Result()
		if err != nil || !set {
			c.AbortWithStatusJSON(401, gin.H{"error": "replayed request"})
			return
		}

		c.Next()
	}
}
