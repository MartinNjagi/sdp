package routers

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"net/http"
	"sdp/connections"
	"sdp/controllers"
	"sdp/controllers/dlr"
	"sdp/data"

	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

// App is the application container. Every service and controller is a field
// here, constructed once in Initialize and shared via dependency injection.
// Nothing is initialised with init() or package-level vars.
type App struct {
	cfg *data.AppConfig
	db  *gorm.DB
	rdc *redis.Client

	// SDP is the Service Delivery Platform gateway. It owns the worker pool,
	// publisher, and DLR reconcile. Exposed so main.go can call Start/Stop.
	SDP *controllers.SDP

	s3Client *s3.Client
}

// Initialize constructs every dependency in order and wires them together.
// Called once from main.go before the HTTP server starts.
func (a *App) Initialize(
	ctx context.Context,
	cfg *data.AppConfig,
	db *gorm.DB,
	rdc *redis.Client,
	RMQManager *connections.RMQManager,
	s3Client *s3.Client,
	client *http.Client,
) {
	a.cfg = cfg
	a.db = db
	a.rdc = rdc

	// --- SDP ----------------------------------------------------------------
	// Pass only the primitives the SDP needs. It builds its own subcomponents
	// internally — the App doesn't need to know about workers or dispatchers.
	sdp, err := controllers.New(ctx, cfg, db, rdc, RMQManager, s3Client, client)
	if err != nil {
		logrus.Fatalf("Failed to initialise SDP: %v", err)
	}
	a.SDP = sdp
	a.s3Client = s3Client

	// Note: SDP.Start() is called from main.go AFTER the HTTP server goroutine
	// is running, so the DLR webhook routes are live before workers begin
	// consuming — avoiding a race where a DLR arrives before the route exists.
}

// SetupRouter registers all Gin routes and returns the configured engine.
func (a *App) SetupRouter() *gin.Engine {
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(gin.Logger())

	// --- Health -------------------------------------------------------------
	r.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok"})
	})

	// --- API v1 -------------------------------------------------------------
	v1 := r.Group("/api/v1")
	{
		// Your existing Engine routes (auth, wallet, campaigns, etc.) go here.
		// Example:
		//   v1.POST("/auth/login",    authHandler.Login)
		//   v1.POST("/campaigns",     campaignHandler.Create)
		_ = v1 // placeholder — remove when first route is added
	}

	// --- SDP Webhooks -------------------------------------------------------
	// These endpoints receive inbound calls from MNOs — no auth middleware,
	// but production deployments should whitelist MNO IP ranges at the
	// load-balancer / firewall level.
	dlrHandler := dlr.NewHandler(a.SDP.Reconciler())

	webhooks := r.Group("/webhooks")
	{
		// Africa's Talking DLR callback.
		// Configure this URL in the AT dashboard:
		//   https://account.africastalking.com → SMS → Delivery Reports
		webhooks.POST("/dlr/at", dlrHandler.ATDeliveryReport)

		// Generic JSON DLR — for new MNOs during integration testing.
		webhooks.POST("/dlr/generic", dlrHandler.GenericDeliveryReport)
	}

	return r
}
