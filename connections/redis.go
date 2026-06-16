package connections

import (
	"context"
	"crypto/tls"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
	"sdp/data"
)

var (
	DB  *gorm.DB
	RDB *redis.Client
	Ctx = context.Background()
)

func InitRedis(Config *data.AppConfig) *redis.Client {
	addr := Config.RedisAddr

	opts := &redis.Options{
		Addr:     addr,
		Password: Config.RedisPassword,
		DB:       Config.RedisDB,
	}

	// Aiven Valkey/Redis typically requires TLS
	switch Config.Env {
	case "staging", "production":
		opts.TLSConfig = &tls.Config{
			MinVersion: tls.VersionTLS12,
		}
	}

	RDB = redis.NewClient(opts)

	if err := RDB.Ping(Ctx).Err(); err != nil {
		logrus.Fatalf("Failed to connect to Redis: %v", err)
	}

	logrus.Info("✓ Redis connected successfully")
	return RDB
}
