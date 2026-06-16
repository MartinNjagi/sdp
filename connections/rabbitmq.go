package connections

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sirupsen/logrus"
	"sdp/data"
)

func InitRMQ(cfg *data.AppConfig) *amqp.Connection {

	host := cfg.RMQHost
	user := cfg.RMQUser
	pass := cfg.RMQPassword
	port := cfg.RMQPort
	vhost := cfg.RMQVHost

	uri := fmt.Sprintf("amqps://%s:%s@%s:%d/%s", user, pass, host, port, vhost)

	if cfg.Env == "local" {
		uri = fmt.Sprintf("amqp://%s:%s@%s:%d", user, pass, host, port)
	}

	conn, err := amqp.Dial(uri)

	if err != nil {
		logrus.WithFields(logrus.Fields{data.DESCRIPTION: "got error connecting to rabbitmq"}).Errorf(err.Error())

		return nil
	}
	logrus.Info("✓ RabbitMQ connected successfully")

	return conn
}
