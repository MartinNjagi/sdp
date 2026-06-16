package connections

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"sdp/data"
)

func InitDB(cfg *data.AppConfig) *gorm.DB {

	dsn := buildDSN(cfg)

	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		logrus.Fatalf("DB connection failed: %v", err)
	}

	DB = db
	logrus.Info("✓ Database connected")

	return DB
}

func buildDSN(cfg *data.AppConfig) string {

	var tlsPart string

	switch cfg.Env {
	case "local":
		tlsPart = ""

	case "development", "staging":
		tlsPart = "&tls=skip-verify"

	case "production":
		tlsPart = "&tls=true"
	}

	return fmt.Sprintf(
		"%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=true&loc=Local%s",
		cfg.DBUser,
		cfg.DBPassword,
		cfg.DBHost,
		cfg.DBPort,
		cfg.DBName,
		tlsPart,
	)
}
