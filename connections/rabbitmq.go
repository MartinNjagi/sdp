package connections

import (
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sirupsen/logrus"
	"sdp/data"
)

// RMQManager wraps the AMQP connection to provide auto-reconnect capabilities.
type RMQManager struct {
	URL         string
	Conn        *amqp.Connection
	NotifyClose chan *amqp.Error
	Reconnect   chan bool // Broadcast channel for workers
}

// InitRMQ builds the URI, initializes the manager, and establishes the first connection.
func InitRMQ(cfg *data.AppConfig) *RMQManager {
	host := cfg.RMQHost
	user := cfg.RMQUser
	pass := cfg.RMQPassword
	port := cfg.RMQPort
	vhost := cfg.RMQVHost

	// Note: URL encoding might be required for usernames/passwords with special characters.
	uri := fmt.Sprintf("amqps://%s:%s@%s:%d/%s", user, pass, host, port, vhost)

	if cfg.Env == "local" {
		// Usually, you still want the vhost locally (e.g., /%2f), but matching your original logic:
		uri = fmt.Sprintf("amqp://%s:%s@%s:%d", user, pass, host, port)
	}

	manager := &RMQManager{
		URL:       uri,
		Reconnect: make(chan bool),
	}

	// Block until the initial connection succeeds
	manager.Connect()

	return manager
}

// Connect attempts to dial RabbitMQ, blocking and retrying every 5 seconds if it fails.
func (m *RMQManager) Connect() {
	for {
		conn, err := amqp.Dial(m.URL)
		if err == nil {
			m.Conn = conn
			m.NotifyClose = make(chan *amqp.Error, 1)
			m.Conn.NotifyClose(m.NotifyClose)

			logrus.Info("✓ RabbitMQ connected successfully")

			// Start listening for unexpected disconnects
			go m.handleReconnect()
			return
		}

		logrus.WithFields(logrus.Fields{
			data.DESCRIPTION: "got error connecting to rabbitmq",
		}).Errorf("%v. Retrying in 5 seconds...", err)

		time.Sleep(5 * time.Second)
	}
}

// handleReconnect waits for the connection to drop, rebuilds it, and notifies workers.
func (m *RMQManager) handleReconnect() {
	err := <-m.NotifyClose
	if err != nil {
		logrus.Errorf("RabbitMQ connection closed: %v. Attempting to reconnect...", err)

		// This will block until the connection is successfully re-established
		m.Connect()

		logrus.Info("Broadcasting reconnect signal to workers...")
		// Send a non-blocking signal to wake up paused workers
		select {
		case m.Reconnect <- true:
		default:
		}
	}
}
