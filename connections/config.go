package connections

import (
	"encoding/json"
	"github.com/joho/godotenv"
	"github.com/sirupsen/logrus"
	"io"
	"os"
	"sdp/data"
	"strconv"
)

// InitConfig reads all environment variables and returns a populated Config.
// Missing required values are logged as fatal — the application should not
// start with a broken configuration.
func InitConfig() *data.AppConfig {

	env := os.Getenv("APP_ENV")

	// ONLY load .env locally
	if env == "local" || env == "development" {
		_ = godotenv.Load()
	}

	cfg := &data.AppConfig{
		Env:        getEnv("APP_ENV", "development"),
		ServerHost: getEnv("SERVER_HOST", ""),
		ServerPort: getEnv("SERVER_PORT", "8080"),

		DBHost:     getEnv("DB_HOST", "localhost"),
		DBPort:     getEnv("DB_PORT", "5432"),
		DBName:     mustEnv("DB_NAME"),
		DBUser:     mustEnv("DB_USER"),
		DBPassword: mustEnv("DB_PASSWORD"),
		DBSSLMode:  getEnv("DB_SSL_MODE", "disable"),

		RedisAddr:     getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPassword: getEnv("REDIS_PASSWORD", ""),
		RedisDB:       getEnvInt("REDIS_DB", 0),

		AMQPURL: getEnv("AMQP_URL", "amqp://guest:guest@localhost:5672/"),

		WorkerPoolVIP:      getEnvInt("WORKER_POOL_VIP", 10),
		WorkerPoolBulk:     getEnvInt("WORKER_POOL_BULK", 2),
		WorkerPoolStandard: getEnvInt("WORKER_POOL_STANDARD", 5),

		ATAPIKey:   getEnv("AT_API_KEY", ""),
		ATUsername: getEnv("AT_USERNAME", ""),
		ATBaseURL:  getEnv("AT_BASE_URL", "https://api.africastalking.com/version1/messaging"),

		SMPPHost:     getEnv("SMPP_HOST", ""),
		SMPPPort:     getEnv("SMPP_PORT", "2775"),
		SMPPSystemID: getEnv("SMPP_SYSTEM_ID", ""),
		SMPPPassword: getEnv("SMPP_PASSWORD", ""),
		SMPPMode:     getEnv("SMPP_MODE", "transceiver"),

		JWTSecret: mustEnv("JWT_SECRET"),

		AWSRegion:          getEnv("AWS_REGION", ""),
		AWSAccessKeyID:     getEnv("AWS_ACCESS_KEY_ID", ""),
		AWSSecretAccessKey: getEnv("AWS_SECRET_ACCESS_KEY", ""),
		S3Bucket:           getEnv("S3_BUCKET", ""),

		MinioEndpoint:  getEnv("S3_ENDPOINT", ""),
		MinioAccessKey: getEnv("S3_ACCESS_KEY", ""),
		MinioSecretKey: getEnv("S3_SECRET_KEY", ""),

		WalletFlushInterval: getEnvInt("WALLET_FLUSH_INTERVAL", 30),
		WalletServiceURL:    getEnv("WALLET_SERVICE_URL", ""),
		WalletBalanceURL:    getEnv("WALLET_BALANCE_URL", ""),

		SSEChannel: getEnv("SSE_REDIS_CHANNEL", "ws:messages"),

		SDPCpID:            getEnv("SDP_CPID", ""),
		SDPSourceAddress:   getEnv("SDP_SOURCE_ADDRESS", ""),
		SDPBulkSMSURL:      getEnv("SDP_BULK_SMS_URL", ""),
		SDPBulkDLRURL:      getEnv("SDP_BULK_SMS_DLR_URL", ""),
		SDPBulkChannel:     getEnv("SDP_BULK_SMS_CHANNEL", "sms"),
		SDPSendSMSURL:      getEnv("SDP_SENDSMS_URL", ""),
		SDPSendSMSChannel:  getEnv("SDP_SENDSMS_CHANNEL", "sms"),
		SDPTokenRedisKey:   getEnv("SDP_TOKEN_REDIS_KEY", "sdp:token"),
		SDPRefreshTokenURL: getEnv("SDP_REFRESH_TOKEN_URL", ""),
		SDPNewTokenURL:     getEnv("SDP_NEW_TOKEN_URL", ""),
		SDPNewUsername:     getEnv("SDP_USERNAME", ""),
		SDPNewPassword:     getEnv("SDP_PASSWORD", ""),
	}

	// Parse MNO_ROUTES JSON.
	cfg.MNORoutes = parseMNORoutes(getEnv("MNO_ROUTES", "[]"))

	return cfg
}

// --------------------------------------------------------------------------
// Helpers
// --------------------------------------------------------------------------

// getEnv returns the value of key or fallback if unset / empty.
func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// mustEnv returns the value of key or fatals if unset / empty.
// Use for credentials and settings the app cannot function without.
func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		logrus.Fatalf("Required environment variable %q is not set", key)
	}
	return v
}

// getEnvInt parses an integer environment variable, returning fallback on
// parse failure or if the variable is unset.
func getEnvInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		logrus.Warnf("Config: %q=%q is not a valid integer — using default %d", key, v, fallback)
		return fallback
	}
	return n
}

// parseMNORoutes unmarshals the MNO_ROUTES JSON array.
// Logs a fatal if the JSON is set but malformed — a bad routing table
// means every message will fail, so it's better to crash early.
func parseMNORoutes(raw string) []data.RouteConfig {
	if raw == "" || raw == "[]" {
		logrus.Warn("Config: MNO_ROUTES is empty — no messages will be routable")
		return nil
	}
	var routes []data.RouteConfig
	if err := json.Unmarshal([]byte(raw), &routes); err != nil {
		logrus.Fatalf("Config: MNO_ROUTES is not valid JSON: %v", err)
	}
	logrus.Infof("Config: Loaded %d MNO route(s)", len(routes))
	return routes
}

func LoadLogging() {
	logrus.SetFormatter(&logrus.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: data.FrontEndTimeFormat,
	})

	logrus.SetOutput(os.Stdout)

	logDir := os.Getenv("LOG_DIR")
	if logDir == "" {
		logrus.Info("logging mode: stdout only")
		return
	}

	if err := os.MkdirAll(logDir, 0755); err != nil {
		logrus.Warnf("failed to create log dir: %v", err)
		return
	}

	file, err := os.OpenFile(
		logDir+"/app.log",
		os.O_CREATE|os.O_WRONLY|os.O_APPEND,
		0644,
	)
	if err != nil {
		logrus.Warnf("failed to open log file: %v", err)
		return
	}

	logrus.SetOutput(io.MultiWriter(os.Stdout, file))
	logrus.Infof("logging mode: stdout + %s/app.log", logDir)
}
