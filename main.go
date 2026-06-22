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
	routers "sdp/router"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
)

// @title           sms-service API Backend
// @version         1.0
// @description     This is a REST server for a Gin-based application.
// @termsOfService  https://dreamhubtech.com/terms/

// @contact.name   API Support
// @contact.url    https://www.dreamhubtech.com/support
// @contact.email  support@dreamhubtech.com

// @license.name  Apache 2.0
// @license.url   https://www.apache.org/licenses/LICENSE-2.0.html

// @BasePath  /api/v1
// @securityDefinitions.apikey BearerAuth
// @in header
// @name Authorization

func main() {

	// ----- 1. Context & Signal Handling -------------------------------------
	// Replace manual 'quit' channel with NotifyContext. This automatically
	// cancels the context if the app receives SIGINT (Ctrl+C) or SIGTERM (Docker stop).
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
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
	rdc := connections.InitRedis(cfg)
	amqp := connections.InitRMQ(cfg)
	httpClient := connections.NewHTTP(cfg.InternalServiceToken, 10*time.Second)
	s3Client, err := connections.InitStorageClient(ctx, cfg)
	if err != nil {
		logrus.Fatalf("Failed to initialize MinIO Client: %v", err)
	}

	// ----- 4. Application container -----------------------------------------
	var app routers.App
	app.Initialize(ctx, cfg, db, rdc, amqp, s3Client, httpClient)

	// Init token Refresh
	var conn connections.SDP
	conn.InitSDPToken(ctx, rdc, cfg)

	// ----- 5. HTTP Server ---------------------------------------------------
	r := app.SetupRouter()

	addr := cfg.ServerHost + ":" + cfg.ServerPort
	logrus.Infof("🚀 SDP API starting on %s", addr)

	srv := &http.Server{
		Addr:    addr,
		Handler: r,
	}

	// HTTP Fail Mechanism
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logrus.Errorf("HTTP server crashed: %v", err)
			cancel() // Trigger global shutdown
		}
	}()

	// ----- 6. Start SDP worker pool -----------------------------------------
	// Worker Fail Mechanism: Run in a goroutine to prevent the hangup!
	go func() {
		logrus.Info("Starting SDP worker pool...")
		app.SDP.Start() // Calling your current start method

	}()

	// ----- 7. Wait for shutdown signal --------------------------------------
	// This blocks the main thread cleanly until an OS signal arrives OR
	// one of the cancel() functions above is triggered.
	<-ctx.Done()

	logrus.Warn("Shutdown signal received. Shutting down server gracefully...")

	// ----- 8. Graceful shutdown ---------------------------------------------
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	// a. Stop accepting new HTTP requests.
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logrus.Errorf("Server forced to shutdown: %v", err)
	}

	// b. Drain the SDP worker pool (ACK in-flight messages).
	app.SDP.Stop()

	logrus.Info("Server stopped cleanly ✅")
}
