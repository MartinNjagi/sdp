package middleware

import (
	"github.com/google/uuid"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

func RequestLogger() gin.HandlerFunc {
	return func(c *gin.Context) {

		requestID := uuid.NewString()

		// attach to request context
		c.Set("request_id", requestID)
		c.Writer.Header().Set("X-Request-ID", requestID)

		start := time.Now()

		c.Next()

		duration := time.Since(start)

		entry := logrus.WithFields(logrus.Fields{
			"request_id": requestID,
			"method":     c.Request.Method,
			"path":       c.Request.URL.Path,
			"status":     c.Writer.Status(),
			"latency":    duration.String(),
			"ip":         c.ClientIP(),
			"user_i":     getRealIP(c),
			"userAgent":  c.Request.UserAgent(),
		})

		if len(c.Errors) > 0 {
			entry.WithField("errors", c.Errors.String()).
				Error("http_request")
			return
		}

		switch {
		case c.Writer.Status() >= 500:
			entry.Error("http_request")

		case c.Writer.Status() >= 400:
			entry.Warn("http_request")

		default:
			entry.Info("http_request")
		}
	}
}

func getRealIP(c *gin.Context) string {
	if ip := c.GetHeader("X-Real-IP"); ip != "" {
		return ip
	}
	return c.RemoteIP()
}
