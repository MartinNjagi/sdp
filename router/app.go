package routers

import (
	"context"
	"net/http"

	"sdp/connections"
	"sdp/controllers"
	"sdp/controllers/dlr"
	"sdp/data"
	"sdp/middleware"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

type App struct {
	Config   *data.AppConfig
	DB       *gorm.DB
	Redis    *redis.Client
	SDP      *controllers.SDP
	S3Client *s3.Client
}

// Initialize constructs dependencies and wires them together before the HTTP server starts.
func (a *App) Initialize(
	ctx context.Context,
	cfg *data.AppConfig,
	db *gorm.DB,
	rdc *redis.Client,
	rmq *connections.RMQManager,
	s3Client *s3.Client,
	client *http.Client,
) {
	a.Config = cfg
	a.DB = db
	a.Redis = rdc
	a.S3Client = s3Client

	sdp, err := controllers.New(ctx, cfg, db, rdc, rmq, s3Client, client)
	if err != nil {
		logrus.Fatalf("Failed to initialise SDP: %v", err)
	}
	a.SDP = sdp
}

// SetupRouter registers all Gin routes and applies security middlewares.
func (a *App) SetupRouter() *gin.Engine {
	r := gin.New()
	err := r.SetTrustedProxies([]string{
		"127.0.0.1",
		"::1",
	})
	if err != nil {
		logrus.Fatal(err)
	}
	r.Use(gin.Recovery())
	r.Use(middleware.RequestLogger())
	r.Use(middleware.CaptureRawBodyMiddleware())

	// --- 1. Health Check ---
	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok", "service": "sdp"})
	})

	// --- 2. Internal Microservice API ---
	api := r.Group("/api/v1")
	// If the SDP exposes endpoints to the SMS engine, uncomment HMAC protection:
	// api.Use(middleware.VerifySignature(a.Config.InternalServiceToken, a.Redis))
	{
		_ = api // Placeholder for future internal routes
	}

	// --- 3. Public Webhooks (Protected by Secret & IP Whitelist) ---
	dlrHandler := dlr.NewHandler(a.SDP.Reconciler())

	webhooks := r.Group("/webhooks/:secret")
	webhooks.Use(middleware.WebhookGuard())
	{
		webhooks.POST("/dlr/at", dlrHandler.ATDeliveryReport)
		webhooks.POST("/dlr/generic", dlrHandler.GenericDeliveryReport)

		//todo Future: webhooks.POST("/dlr/safaricom", dlrHandler.SafaricomDeliveryReport)
	}

	return r
}
