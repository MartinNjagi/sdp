package data

// AppConfig is the single source of truth for all runtime configuration.
// // Values are read from environment variables at startup. No config file
// // is used — keeps the deployment model simple (12-factor).
type AppConfig struct {
	// ----- Server -----------------------------------------------------------
	Env        string // APP_ENV: "development" | "staging" | "production"
	ServerHost string // SERVER_HOST: default ""  (all interfaces)
	ServerPort string // SERVER_PORT: default "8080"

	// ----- Database ---------------------------------------------------------
	DBHost     string // DB_HOST
	DBPort     string // DB_PORT
	DBName     string // DB_NAME
	DBUser     string // DB_USER
	DBPassword string // DB_PASSWORD
	DBSSLMode  string // DB_SSL_MODE: "disable" | "require"

	// ----- Redis ------------------------------------------------------------
	RedisAddr     string // REDIS_ADDR: default "localhost:6379"
	RedisPassword string // REDIS_PASSWORD
	RedisDB       int    // REDIS_DB: default 0

	// ----- RabbitMQ ---------------------------------------------------------
	AMQPURL string // AMQP_URL: e.g. "amqp://guest:guest@localhost:5672/"

	// ----- Worker Pool ------------------------------------------------------
	// WorkerPoolSize controls how many goroutines consume from the outbound queue.
	// Rule of thumb: start at 5, raise until CPU or MNO TPS ceiling is hit.
	WorkerPoolVIP      int // WORKER_POOL_SIZE: default 5
	WorkerPoolStandard int // WORKER_POOL_SIZE: default 5
	WorkerPoolBulk     int // WORKER_POOL_SIZE: default 5

	// ----- MNO Routing ------------------------------------------------------
	// MNORoutes is parsed from MNO_ROUTES (JSON array). See RouteConfig above.
	MNORoutes []RouteConfig

	// ----- Africa's Talking (HTTP Dispatcher) --------------------------------
	ATAPIKey   string // AT_API_KEY
	ATUsername string // AT_USERNAME
	ATBaseURL  string // AT_BASE_URL: default "https://api.africastalking.com/version1/messaging"

	// ----- SMPP (future) ----------------------------------------------------
	SMPPHost     string // SMPP_HOST: e.g. "smpp.safaricom.co.ke"
	SMPPPort     string // SMPP_PORT: default "2775"
	SMPPSystemID string // SMPP_SYSTEM_ID
	SMPPPassword string // SMPP_PASSWORD
	SMPPMode     string // SMPP_MODE: "transceiver" | "transmitter"

	// ----- JWT / Auth -------------------------------------------------------
	JWTSecret string // JWT_SECRET

	// ----- AWS S3 (file uploads) --------------------------------------------
	AWSRegion          string // AWS_REGION
	AWSAccessKeyID     string // AWS_ACCESS_KEY_ID
	AWSSecretAccessKey string // AWS_SECRET_ACCESS_KEY
	S3Bucket           string // S3_BUCKET

	// ----- Minio S3 (file uploads) --------------------------------------------
	MinioEndpoint  string // AWS_REGION
	MinioAccessKey string // AWS_ACCESS_KEY_ID
	MinioSecretKey string // AWS_SECRET_ACCESS_KEY
	MinioBucket    string // S3_BUCKET

	WalletServiceURL    string
	WalletFlushInterval int
	WalletBalanceURL    string

	// ----- Safaricom SDP -------------------------------------------------------
	SDPCpID           string // SDP_CPID
	SDPSourceAddress  string // SDP_SOURCE_ADDRESS
	SDPBulkSMSURL     string // SDP_BULK_SMS_URL
	SDPBulkDLRURL     string // SDP_BULK_SMS_DLR_URL
	SDPBulkChannel    string // SDP_BULK_SMS_CHANNEL:  default "sms"
	SDPSendSMSURL     string // SDP_SENDSMS_URL
	SDPSendSMSChannel string // SDP_SENDSMS_CHANNEL:   default "sms"
	SDPTokenRedisKey  string // SDP_TOKEN_REDIS_KEY:   default "sdp:token"

	// ----- SSE / Redis Pub/Sub -------------------------------------------------------
	// The SDP publishes DLR events and campaign progress to this channel.
	// The Node BFF already subscribes to it — no extra wiring needed beyond
	// a plain PUBLISH. As long as the event lands here, it gets routed.
	SSEChannel string // SSE_REDIS_CHANNEL: default "ws:messages"

}

// RouteConfig defines one MNO entry in the routing table.
// Loaded from the MNO_ROUTES environment variable as a JSON array.
//
// Example env value:
//
//	MNO_ROUTES=[{"prefix":"2547","name":"safaricom","dispatcher":"http_at","tps_limit":50},
//	            {"prefix":"2541","name":"airtel","dispatcher":"http_at","tps_limit":20}]
type RouteConfig struct {
	Prefix     string `json:"prefix"`     // MSISDN prefix, e.g. "2547"
	Name       string `json:"name"`       // Human label, e.g. "safaricom"
	Dispatcher string `json:"dispatcher"` // "http_at" | "smpp"
	TPSLimit   int    `json:"tps_limit"`  // Max messages per second on this MNO
}
