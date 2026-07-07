package middleware

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	"github.com/redis/go-redis/v9"
	"io"
	"net/http"
	"sdp/data"
	"strings"
	"time"
)

type UserContext struct {
	UserID      uint
	Username    string
	Permissions []data.Permission
}

var RedisKeyUserPermissions = "user:permissions:%d" // user_id

// JWTAuthMiddleware verifies the token and sets context variables
func JWTAuthMiddleware(jwtSecret string) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		// Extract token from Authorization header (or cookie, depending on your setup)
		authHeader := ctx.GetHeader("Authorization")
		if authHeader == "" || !strings.HasPrefix(authHeader, "Bearer ") {
			ctx.JSON(http.StatusUnauthorized, gin.H{"error": "Missing or invalid authorization header"})
			ctx.Abort()
			return
		}

		tokenString := strings.TrimPrefix(authHeader, "Bearer ")

		// Parse and validate the token
		token, err := jwt.ParseWithClaims(tokenString, &data.CustomClaims{}, func(t *jwt.Token) (interface{}, error) {
			return []byte(jwtSecret), nil
		})

		if err != nil || !token.Valid {
			ctx.JSON(http.StatusUnauthorized, gin.H{"error": "Invalid or expired token"})
			ctx.Abort()
			return
		}

		// Extract claims and inject them into the Gin context
		if claims, ok := token.Claims.(*data.CustomClaims); ok {
			ctx.Set("user_id", claims.UserID)
			ctx.Set("username", claims.Username)
			ctx.Set("client_id", claims.ClientID)
			ctx.Set("permission_ids", claims.PermissionIDs)
			ctx.Next()
		} else {
			ctx.JSON(http.StatusUnauthorized, gin.H{"error": "Failed to parse token claims"})
			ctx.Abort()
			return
		}
	}
}

// RequirePermission is a strict RBAC middleware for specific routes
func RequirePermission(requiredPermID int) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		// Retrieve permission IDs from context (injected by JWTAuthMiddleware)
		permIDs, exists := ctx.Get("permission_ids")
		if !exists {
			ctx.JSON(http.StatusForbidden, gin.H{"error": "Access denied"})
			ctx.Abort()
			return
		}

		hasPermission := false
		for _, id := range permIDs.([]uint) { // Assuming your IDs are uints
			if id == uint(requiredPermID) {
				hasPermission = true
				break
			}
		}

		if !hasPermission {
			ctx.JSON(http.StatusForbidden, gin.H{"error": "Insufficient permissions to perform this action"})
			ctx.Abort()
			return
		}

		ctx.Next()
	}
}

func CaptureRawBodyMiddleware() gin.HandlerFunc {
	return func(ctx *gin.Context) {
		// Enforce a 5MB limit to prevent OOM attacks
		const maxBodyBytes = 5 * 1024 * 1024
		bodyBytes, err := io.ReadAll(io.LimitReader(ctx.Request.Body, maxBodyBytes))
		if err != nil {
			ctx.AbortWithStatusJSON(http.StatusRequestEntityTooLarge, gin.H{"error": "request body too large"})
			return
		}

		ctx.Set("rawBody", string(bodyBytes))
		ctx.Request.Body = io.NopCloser(bytes.NewBuffer(bodyBytes)) // reset for downstream
		ctx.Next()
	}
}

// JWTAuthRedis validates JWT and loads permissions purely from Redis
func JWTAuthRedis(rdb *redis.Client, jwtSecret []byte) gin.HandlerFunc {
	return func(c *gin.Context) {
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" || !strings.HasPrefix(authHeader, "Bearer ") {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "missing or invalid authorization header"})
			c.Abort()
			return
		}

		tokenStr := strings.TrimPrefix(authHeader, "Bearer ")

		// Parse JWT
		token, err := jwt.ParseWithClaims(tokenStr, &data.CustomClaims{}, func(token *jwt.Token) (interface{}, error) {
			if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
			}
			return jwtSecret, nil
		})
		if err != nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": err.Error(),
			})
			return
		}

		claims, ok := token.Claims.(*data.CustomClaims)
		if !ok || !token.Valid {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": "invalid token claims",
			})
			return
		}

		ctx := c.Request.Context()

		// ---- Redis session validation ----
		sessionKey := fmt.Sprintf("user:%d:token", claims.UserID)
		storedJTI, err := rdb.Get(ctx, sessionKey).Result()
		if errors.Is(err, redis.Nil) {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "session expired or revoked"})
			c.Abort()
			return
		} else if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "redis error"})
			c.Abort()
			return
		}

		if storedJTI != claims.ID {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "token no longer valid"})
			c.Abort()
			return
		}

		// Optional expiration sanity check
		if claims.ExpiresAt != nil && claims.ExpiresAt.Time.Before(time.Now()) {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "token expired"})
			c.Abort()
			return
		}

		// ---- Load permissions from Redis ----
		permKey := fmt.Sprintf(RedisKeyUserPermissions, claims.UserID)

		val, err := rdb.Get(ctx, permKey).Result()
		if errors.Is(err, redis.Nil) {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "permissions expired, please retry login"})
			c.Abort()
			return
		} else if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "redis error"})
			c.Abort()
			return
		}

		var permissions []data.Permission
		if err := json.Unmarshal([]byte(val), &permissions); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to decode permissions"})
			c.Abort()
			return
		}

		// Attach user context
		c.Set("user", &UserContext{
			UserID:      claims.UserID,
			Username:    claims.Username,
			Permissions: permissions,
		})
		c.Set("user_id", claims.UserID)
		c.Set("client_id", claims.ClientID) // <-- THIS IS THE MISSING PIECE
		c.Set("username", claims.Username)
		c.Set("permissions", permissions)

		c.Next()
	}
}

// RoleAuth checks if the user has the required permission name
func RoleAuth(permissionName string) gin.HandlerFunc {
	return func(c *gin.Context) {
		val, exists := c.Get("permissions")
		if !exists {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "user permissions not found"})
			return
		}

		perms, ok := val.([]data.Permission)
		if !ok {
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "invalid permissions format"})
			return
		}

		for _, p := range perms {
			if p.Name == permissionName {
				c.Next()
				return
			}
		}

		c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "insufficient permissions"})
	}
}
