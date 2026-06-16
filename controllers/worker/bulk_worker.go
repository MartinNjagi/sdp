package worker

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sdp/controllers/mno_router"
	"sdp/controllers/publisher"
	"sdp/data"
	"strings"
	"sync"
	"time"

	amqplib "github.com/rabbitmq/amqp091-go"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

// BulkWorker consumes BulkEnvelopes from the bulk queue.
// For each envelope it:
//  1. Fetches the contact CSV from S3/Minio via the FileURL.
//  2. Parses each row and compiles the message (template + replacements).
//  3. Resolves the destination carrier and prices the message in credits.
//  4. Writes an Outbox row per contact.
//  5. Fan-out publishes a DispatchEnvelope per contact to the standard queue.
//
// The BulkWorker never touches the Telco — it is a pure expander.
type BulkWorker struct {
	ctx        context.Context
	ch         *amqplib.Channel
	pub        *publisher.Publisher
	router     *mno_router.Router
	costEngine *data.CostEngine
	db         *gorm.DB
	httpClient *http.Client
	poolSize   int
}

// newBulkWorker constructs a BulkWorker with its own dedicated AMQP channel.
func newBulkWorker(
	ctx context.Context,
	conn *amqplib.Connection,
	pub *publisher.Publisher,
	router *mno_router.Router,
	costEngine *data.CostEngine,
	db *gorm.DB,
	poolSize int,
) (*BulkWorker, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("bulk worker: open channel: %w", err)
	}

	// Bulk messages are heavy — fetch only one at a time per goroutine so a
	// slow S3 download doesn't starve the broker.
	if err := ch.Qos(1, 0, false); err != nil {
		_ = ch.Close()
		return nil, fmt.Errorf("bulk worker: set QoS: %w", err)
	}

	return &BulkWorker{
		ctx:        ctx,
		ch:         ch,
		pub:        pub,
		router:     router,
		costEngine: costEngine,
		db:         db,
		poolSize:   poolSize,
		httpClient: &http.Client{
			Timeout: 60 * time.Second, // S3 downloads can be large
		},
	}, nil
}

func (w *BulkWorker) start(wg *sync.WaitGroup) {
	for i := range w.poolSize {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			w.consume(id)
		}(i)
	}
	logrus.Infof("[BulkWorker] Pool of %d goroutines started on %s", w.poolSize, publisher.QueueBulk)
}

func (w *BulkWorker) stop() {
	if w.ch != nil {
		_ = w.ch.Close()
	}
}

func (w *BulkWorker) consume(id int) {
	tag := fmt.Sprintf("bulk-worker-%d", id)
	deliveries, err := w.ch.Consume(
		publisher.QueueBulk, tag, false, false, false, false, nil,
	)
	if err != nil {
		logrus.Errorf("[BulkWorker-%d] Start consume: %v", id, err)
		return
	}

	for {
		select {
		case <-w.ctx.Done():
			return
		case d, ok := <-deliveries:
			if !ok {
				return
			}
			w.handle(id, d)
		}
	}
}

func (w *BulkWorker) handle(workerID int, d amqplib.Delivery) {
	log := logrus.WithField("bulk_worker", workerID)

	var env data.BulkEnvelope
	if err := json.Unmarshal(d.Body, &env); err != nil {
		log.Errorf("Malformed BulkEnvelope — dead-lettering: %v", err)
		_ = d.Nack(false, false)
		return
	}

	log = log.WithFields(logrus.Fields{
		"campaign_id": env.CampaignID,
		"client_id":   env.ClientID,
	})

	log.Infof("Processing bulk campaign file_url=%s", env.FileURL)

	// 1. Fetch contact list from S3/Minio.
	contacts, err := w.fetchContacts(env.FileURL)
	if err != nil {
		log.Errorf("Fetch contacts: %v", err)
		_ = d.Nack(false, true) // requeue — S3 may be temporarily unavailable
		return
	}

	log.Infof("Fetched %d contacts for campaign_id=%d", len(contacts), env.CampaignID)

	// 2. Fan-out: compile + write Outbox + publish per contact.
	failed := 0
	for _, contact := range contacts {
		if err := w.processContact(env, contact); err != nil {
			log.Errorf("processContact msisdn=%s: %v", contact.MSISDN, err)
			failed++
			// Continue — partial failure is acceptable for bulk campaigns.
		}
	}

	if failed > 0 {
		log.Warnf("campaign_id=%d completed with %d/%d failures", env.CampaignID, failed, len(contacts))
	}

	_ = d.Ack(false)
	log.Infof("campaign_id=%d fan-out complete: %d dispatched, %d failed",
		env.CampaignID, len(contacts)-failed, failed)
}

// processContact compiles a single message, resolves its carrier and
// price, writes the Outbox row, and publishes a DispatchEnvelope to the
// standard queue.
func (w *BulkWorker) processContact(env data.BulkEnvelope, contact Contact) error {
	// Compile message from template + contact-level replacements.
	compiled := compileTemplate(env.TemplateID, contact.Replacements)

	// Resolve the destination carrier so pricing can vary by MNO if the
	// rate table has a carrier-specific override. A routing failure here
	// is the same permanent failure the DispatchWorker would hit later —
	// catching it now avoids paying for an Outbox write and a queue publish
	// for a message that can never be sent.
	route, err := w.router.Resolve(contact.MSISDN)
	if err != nil {
		return fmt.Errorf("resolve route: %w", err)
	}

	priced, err := w.costEngine.Price(data.CostRequest{
		Body:  compiled,
		Class: data.ClassBulk,
		MNO:   route.Name,
	})
	if err != nil {
		return fmt.Errorf("price message: %w", err)
	}

	// Write Outbox row and get the assigned ID.
	outboxID, err := w.writeOutbox(env, contact.MSISDN, compiled, priced.TotalCredits)
	if err != nil {
		return fmt.Errorf("write outbox: %w", err)
	}

	// Publish DispatchEnvelope to the standard queue.
	// Bulk campaigns are never VIP — they go through the standard lane.
	dispatch := data.DispatchEnvelope{
		OutboxID:    outboxID,
		ClientID:    env.ClientID,
		MSISDN:      contact.MSISDN,
		SenderID:    env.SenderID,
		Message:     compiled,
		MessageType: "bulk",
		Cost:        priced.TotalCredits,
	}

	return w.pub.PublishDispatch(w.ctx, dispatch)
}

// --------------------------------------------------------------------------
// Outbox write
// --------------------------------------------------------------------------

func (w *BulkWorker) writeOutbox(env data.BulkEnvelope, msisdn, message string, cost int64) (uint64, error) {
	campaignID := env.CampaignID
	row := map[string]interface{}{
		"client_id":   env.ClientID,
		"campaign_id": campaignID,
		"msisdn":      msisdn,
		"sender_id":   env.SenderID,
		"message":     message,
		"cost":        cost,
		"status":      "PENDING",
		"created_at":  time.Now(),
		"updated_at":  time.Now(),
	}

	result := w.db.Table("outboxes").Create(row)
	if result.Error != nil {
		return 0, result.Error
	}

	// Retrieve the inserted ID.
	var outbox struct {
		ID uint64
	}
	w.db.Table("outboxes").
		Where("client_id = ? AND msisdn = ? AND campaign_id = ?", env.ClientID, msisdn, campaignID).
		Order("created_at DESC").
		Select("id").
		First(&outbox)

	return outbox.ID, nil
}

// --------------------------------------------------------------------------
// S3/Minio contact fetch
// --------------------------------------------------------------------------

// Contact represents a single parsed row from the campaign CSV.
type Contact struct {
	MSISDN       string
	Replacements map[string]string // column_name → value for template substitution
}

// fetchContacts downloads the CSV from the FileURL and parses it into contacts.
// The CSV must have at minimum a "msisdn" column. All other columns are passed
// as template replacements keyed by column header name.
func (w *BulkWorker) fetchContacts(fileURL string) ([]Contact, error) {
	resp, err := w.httpClient.Get(fileURL)
	if err != nil {
		return nil, fmt.Errorf("http get file_url: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("file_url returned %d", resp.StatusCode)
	}

	reader := csv.NewReader(resp.Body)
	reader.TrimLeadingSpace = true

	// Read header row.
	headers, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("read CSV headers: %w", err)
	}

	// Normalise headers to lowercase for consistent lookup.
	for i, h := range headers {
		headers[i] = strings.ToLower(strings.TrimSpace(h))
	}

	msisdnIdx := -1
	for i, h := range headers {
		if h == "msisdn" || h == "phone" || h == "phone_number" {
			msisdnIdx = i
			break
		}
	}
	if msisdnIdx == -1 {
		return nil, fmt.Errorf("CSV missing 'msisdn' column (found: %v)", headers)
	}

	var contacts []Contact
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read CSV row: %w", err)
		}

		if len(row) <= msisdnIdx {
			continue // skip malformed rows
		}

		replacements := make(map[string]string, len(headers))
		for i, h := range headers {
			if i < len(row) {
				replacements[h] = strings.TrimSpace(row[i])
			}
		}

		contacts = append(contacts, Contact{
			MSISDN:       strings.TrimSpace(row[msisdnIdx]),
			Replacements: replacements,
		})
	}

	return contacts, nil
}

// --------------------------------------------------------------------------
// Template compilation
// --------------------------------------------------------------------------

// compileTemplate performs {{key}} substitution on the template body.
// Keys are looked up in replacements; unmatched placeholders are left as-is.
func compileTemplate(template string, replacements map[string]string) string {
	result := template
	for key, value := range replacements {
		result = strings.ReplaceAll(result, "{{"+key+"}}", value)
		result = strings.ReplaceAll(result, "{{ "+key+" }}", value)
	}
	return result
}
