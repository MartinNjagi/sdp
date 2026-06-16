package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sdp/connections"
	"sdp/docs"
	"sdp/router"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
)

// @title           sdp-service API Backend
// @version         1.0
// @description     This is a REST server for a Gin-based application.
// @termsOfService  https://dreamhubtech.com/terms/

// @contact.name   API Support
// @contact.url    https://www.dreamhubtech.com/supportsw
// @contact.email  support@dreamhubtech.com

// @license.name  Apache 2.0
// @license.url   https://www.apache.org/licenses/LICENSE-2.0.html

// @BasePath  /api/v1
// @securityDefinitions.apikey BearerAuth
// @in header
// @name Authorization

func main() {

	// ----- 1. Context -------------------------------------------------------
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// ----- 2. Config & Logger -----------------------------------------------
	loc, err := time.LoadLocation("Africa/Nairobi")
	if err == nil {
		time.Local = loc
	}
	cfg := connections.InitConfig()
	connections.LoadLogging()
	logrus.Infof("Loaded config: environment=%s", cfg.Env)

	docs.SwaggerInfo.Host = fmt.Sprintf("%s:%s", cfg.ServerHost, cfg.ServerPort)
	docs.SwaggerInfo.Schemes = []string{"https"}

	// ----- 3. Connections ---------------------------------------------------
	db := connections.InitDB(cfg)
	rdc := connections.InitRedis()
	amqp := connections.InitRMQ(cfg)

	// ----- 4. Application container -----------------------------------------
	// Initialize wires all services together and constructs the SDP internally.
	// SDP.Start() is intentionally deferred until AFTER the HTTP server is
	// running — the DLR webhook routes must be live before workers begin
	// consuming to prevent DLRs arriving on a 404 endpoint.
	var app router.App
	app.Initialize(ctx, cfg, db, rdc, amqp)

	// ----- 5. HTTP Server ---------------------------------------------------
	r := app.SetupRouter()

	addr := cfg.ServerHost + ":" + cfg.ServerPort
	logrus.Infof("🚀 SDP API starting on %s", addr)

	srv := &http.Server{
		Addr:    addr,
		Handler: r,
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logrus.Fatalf("listen: %s\n", err)
		}
	}()

	// ----- 6. Start SDP worker pool -----------------------------------------
	// Workers start consuming from RabbitMQ only after the HTTP server is up.
	app.SDP.Start()

	// ----- 7. Wait for shutdown signal --------------------------------------
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logrus.Warn("Shutting down server...")

	// ----- 8. Graceful shutdown ---------------------------------------------
	// Order matters:
	//   a. Stop accepting new HTTP requests.
	//   b. Drain the SDP worker pool (ACK in-flight messages).
	//   c. Cancel the background context (crons, SSE loops, etc.).

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		logrus.Fatal("Server forced to shutdown: ", err)
	}

	// Drain workers — give them up to 30 s to finish in-flight dispatches.
	app.SDP.Stop()

	// Signal all remaining background goroutines to exit.
	cancel()

	logrus.Info("Server stopped cleanly ✅")
}
