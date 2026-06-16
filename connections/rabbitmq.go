package connections

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sirupsen/logrus"
	"sdp/data"
)

func InitRMQ(cfg *data.AppConfig) *amqp.Connection {

	uri := cfg.AMQPURL

	conn, err := amqp.Dial(uri)

	if err != nil {
		logrus.WithFields(logrus.Fields{data.DESCRIPTION: "got error connecting to rabbitmq"}).Errorf(err.Error())

		return nil
	}
	logrus.Info("✓ RabbitMQ connected successfully")

	return conn
}
