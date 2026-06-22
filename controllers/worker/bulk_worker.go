package worker

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"sdp/connections"
	"sdp/controllers/mno_router"
	"sdp/controllers/publisher"
	"sdp/controllers/storage"
	"sdp/data"
	"strconv"
	"strings"
	"sync"
	"time"

	amqplib "github.com/rabbitmq/amqp091-go"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

// BulkWorker consumes BulkEnvelopes from the bulk queue.
type BulkWorker struct {
	ctx        context.Context
	RMQManager *connections.RMQManager // Replaces raw amqplib.Connection
	ch         *amqplib.Channel
	pub        *publisher.Publisher
	router     *mno_router.Router
	costEngine *data.CostEngine
	db         *gorm.DB
	s3         *storage.S3Service
	s3Bucket   string
	poolSize   int
}

// newBulkWorker constructs a BulkWorker with its own dedicated AMQP channel.
func newBulkWorker(
	ctx context.Context,
	rmqManager *connections.RMQManager, // Injected Manager
	pub *publisher.Publisher,
	router *mno_router.Router,
	costEngine *data.CostEngine,
	db *gorm.DB,
	s3Svc *storage.S3Service,
	poolSize int,
	s3Bucket string,
) (*BulkWorker, error) {

	// FIX: Use the manager's connection to open the channel
	ch, err := rmqManager.Conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("bulk worker: open channel: %w", err)
	}

	if err := ch.Qos(1, 0, false); err != nil {
		_ = ch.Close()
		return nil, fmt.Errorf("bulk worker: set QoS: %w", err)
	}

	return &BulkWorker{
		ctx:        ctx,
		RMQManager: rmqManager, // Save manager to struct for reconnects
		ch:         ch,
		pub:        pub,
		router:     router,
		costEngine: costEngine,
		db:         db,
		s3:         s3Svc,
		s3Bucket:   s3Bucket,
		poolSize:   poolSize,
	}, nil
}

func (w *BulkWorker) start(wg *sync.WaitGroup) {
	for i := 0; i < w.poolSize; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			w.consume(id)
		}(i)
	}
	logrus.Infof("[BulkWorker] Pool of %d goroutines started on %s", w.poolSize, publisher.QueueBulk)
}

// Phase 1: Tell RabbitMQ to stop sending new messages to this worker pool
func (w *BulkWorker) cancelConsumer() {
	if w.ch != nil {
		// Loop through every worker ID in the pool and cancel its specific tag
		for i := 0; i < w.poolSize; i++ {
			tag := fmt.Sprintf("bulk-worker-%d", i)

			// This stops new deliveries for this specific goroutine,
			// but keeps the channel OPEN so we can still ACK messages we already have.
			if err := w.ch.Cancel(tag, false); err != nil {
				logrus.Warnf("[BulkWorker] Failed to cancel consumer %s: %v", tag, err)
			} else {
				logrus.Infof("[BulkWorker] Cancelled consumer: %s", tag)
			}
		}
	}
}

// Phase 3: Close the channel completely
func (w *BulkWorker) closeChannel() {
	if w.ch != nil {
		_ = w.ch.Close()
	}
}

func (w *BulkWorker) consume(id int) {
	tag := fmt.Sprintf("bulk-worker-%d", id)

	// FIX: Outer loop to handle auto-reconnection
	for {
		// 1. Ensure we have a valid, open channel
		if w.ch == nil || w.ch.IsClosed() {
			newCh, err := w.RMQManager.Conn.Channel()
			if err != nil {
				logrus.Errorf("[BulkWorker-%d] Failed to recreate channel: %v. Retrying...", id, err)
				time.Sleep(2 * time.Second)
				continue
			}

			// Re-apply QoS on the new channel
			_ = newCh.Qos(1, 0, false)
			w.ch = newCh
		}

		// 2. Start Consuming
		deliveries, err := w.ch.Consume(
			publisher.QueueBulk, tag, false, false, false, false, nil,
		)
		if err != nil {
			logrus.Errorf("[BulkWorker-%d] Start consume failed: %v", id, err)
			time.Sleep(2 * time.Second)
			continue
		}

		logrus.Infof("[BulkWorker-%d] Listening", id)

		// 3. Inner process loop
	processLoop:
		for {
			select {
			case <-w.ctx.Done():
				return // App is shutting down

			case d, ok := <-deliveries:
				if !ok {
					// Channel closed! Break out of the inner loop
					logrus.Warnf("[BulkWorker-%d] Channel closed! Waiting for reconnect...", id)
					w.ch = nil // Force recreation on next tick

					// Wait for the RMQManager to signal the connection is back
					<-w.RMQManager.Reconnect
					break processLoop
				}

				w.handle(id, d)
			}
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

	var contacts []Contact
	var fetchErr error

	ContactGroupID, _ := strconv.Atoi(env.ContactGroupID)

	// 1. Fetch contact list based on available configuration
	if env.FileURL != "" {
		log.Infof("Fetching contacts from FileURL=%s", env.FileURL)
		contacts, fetchErr = w.fetchContactsFromFile(env.FileURL)
	} else if ContactGroupID > 0 {
		log.Infof("Fetching contacts from DB ContactGroupID=%d", ContactGroupID)
		contacts, fetchErr = w.fetchContactsFromDB(uint64(ContactGroupID))
	} else {
		log.Error("Neither file_url nor contact_group_id provided in envelope")
		_ = d.Nack(false, false) // Dead-letter permanently
		return
	}

	if fetchErr != nil {
		log.Errorf("Fetch contacts failed: %v", fetchErr)
		_ = d.Nack(false, true) // Requeue — DB or Storage might be temporarily down
		return
	}

	log.Infof("Fetched %d contacts for campaign_id=%d", len(contacts), env.CampaignID)

	// ---Pre-fetch the Template ---
	var templateBody string

	// Assuming env.TemplateID is the primary key of your templates table
	if err := w.db.Table("templates").
		Select("content"). // Just grab the string content you need
		Where("id = ? or name = ?", env.TemplateID, env.TemplateID).
		Scan(&templateBody).Error; err != nil {

		log.Errorf("Failed to fetch template id=%v: %v", env.TemplateID, err)
		_ = d.Nack(false, false) // Dead-letter, cannot proceed without template
		return
	}

	if templateBody == "" {
		log.Errorf("Template id=%v is empty", env.TemplateID)
		_ = d.Nack(false, false)
		return
	}

	// 2. Fan-out: compile + write Outbox + publish per contact.
	failed := 0
	for _, contact := range contacts {
		if err := w.processContact(env, templateBody, contact); err != nil {
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

// Local struct mapping just for the FirstOrCreate check
type PhoneNumber struct {
	ID     uint64 `gorm:"primaryKey;autoIncrement"`
	MSISDN string `gorm:"column:msisdn"`
}

func (w *BulkWorker) processContact(env data.BulkEnvelope, templateBody string, contact Contact) error {
	// PRECHECK: Ensure we have a PhoneID
	if contact.PhoneID == 0 {
		var pn PhoneNumber
		err := w.db.Table("phone_numbers").
			Where("msisdn = ?", contact.MSISDN).
			FirstOrCreate(&pn, map[string]interface{}{"msisdn": contact.MSISDN}).Error

		if err != nil {
			return fmt.Errorf("failed to resolve phone_id: %w", err)
		}
		contact.PhoneID = pn.ID
	}

	// USE THE PRE-FETCHED STRING HERE:
	compiled := compileTemplate(templateBody, contact.Replacements)

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

	// Write Outbox row
	outboxID, err := w.writeOutbox(env, contact.PhoneID, contact.MSISDN, compiled, priced.TotalCredits)
	if err != nil {
		return fmt.Errorf("write outbox: %w", err)
	}

	// Publish DispatchEnvelope
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

func (w *BulkWorker) writeOutbox(env data.BulkEnvelope, phoneID uint64, msisdn, message string, cost int64) (uint64, error) {
	campaignID := env.CampaignID
	row := map[string]interface{}{
		"client_id":   env.ClientID,
		"campaign_id": campaignID,
		"phone_id":    phoneID,
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
	var outbox struct{ ID uint64 }
	w.db.Table("outboxes").
		Where("client_id = ? AND msisdn = ? AND campaign_id = ?", env.ClientID, msisdn, campaignID).
		Order("created_at DESC").
		Select("id").
		First(&outbox)

	return outbox.ID, nil
}

// --------------------------------------------------------------------------
// Contact fetching sources
// --------------------------------------------------------------------------

// Contact represents a single parsed contact with its metadata.
type Contact struct {
	PhoneID      uint64 // <-- Added to hold the DB reference
	MSISDN       string
	Replacements map[string]string // Key → value for template substitution
}

// fetchContactsFromDB queries the normalized address book tables for all
// active members of a contact group. It automatically drops blacklisted numbers.
func (w *BulkWorker) fetchContactsFromDB(groupID uint64) ([]Contact, error) {
	// Struct to capture the specific projection from our JOIN query
	type addressBookEntry struct {
		PhoneID     uint64 `gorm:"column:phone_id"` // <-- Added
		MSISDN      string
		ContactName string
	}

	var entries []addressBookEntry

	// Perform the multi-table JOIN and grab pn.id
	err := w.db.Table("contact_group_members AS cgm").
		Select("pn.id AS phone_id, pn.msisdn, cab.contact_name"). // <-- Selected pn.id
		Joins("JOIN client_address_books AS cab ON cgm.client_id = cab.client_id AND cgm.phone_id = cab.phone_id").
		Joins("JOIN phone_numbers AS pn ON cab.phone_id = pn.id").
		Where("cgm.group_id = ?", groupID).
		Where("cab.is_blacklisted = ?", false).
		Find(&entries).Error

	if err != nil {
		return nil, fmt.Errorf("query contacts from db: %w", err)
	}

	if len(entries) == 0 {
		return nil, fmt.Errorf("no valid, non-blacklisted contacts found for group_id=%d", groupID)
	}

	var contacts []Contact
	for _, entry := range entries {
		msisdn := strings.TrimSpace(entry.MSISDN)
		if msisdn == "" {
			continue
		}

		replacements := map[string]string{
			"msisdn":       msisdn,
			"contact_name": entry.ContactName,
			"name":         entry.ContactName,
		}

		contacts = append(contacts, Contact{
			PhoneID:      entry.PhoneID, // <-- Map the ID
			MSISDN:       msisdn,
			Replacements: replacements,
		})
	}

	return contacts, nil
}

// fetchContactsFromFile streams the CSV using the raw file key and default bucket.
func (w *BulkWorker) fetchContactsFromFile(fileKey string) ([]Contact, error) {
	// Optional safeguard: strip leading slashes if they exist
	fileKey = strings.TrimPrefix(fileKey, "/")

	// Use DownloadByKey instead of the URI parser
	stream, err := w.s3.DownloadByKey(w.ctx, w.s3Bucket, fileKey)
	if err != nil {
		return nil, fmt.Errorf("s3 download key: %w", err)
	}
	defer stream.Close()

	reader := csv.NewReader(stream)
	reader.TrimLeadingSpace = true

	headers, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("read CSV headers: %w", err)
	}

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
			continue
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

// compileTemplate performs substitution on the template body.
// Supports {{key}}, {key}, and [key] formats, ignoring spaces and case.
func compileTemplate(template string, replacements map[string]string) string {
	result := template

	for key, value := range replacements {
		// Because replacements map keys are forced to lowercase during extraction,
		// we generate an uppercase version to match tags like [CODE] or {NAME}.
		upperKey := strings.ToUpper(key)

		// 1. Double curly braces: {{name}} or {{NAME}}
		result = strings.ReplaceAll(result, "{{"+key+"}}", value)
		result = strings.ReplaceAll(result, "{{"+upperKey+"}}", value)
		result = strings.ReplaceAll(result, "{{ "+key+" }}", value)
		result = strings.ReplaceAll(result, "{{ "+upperKey+" }}", value)

		// 2. Single curly braces: {name} or {NAME}
		result = strings.ReplaceAll(result, "{"+key+"}", value)
		result = strings.ReplaceAll(result, "{"+upperKey+"}", value)
		result = strings.ReplaceAll(result, "{ "+key+" }", value)
		result = strings.ReplaceAll(result, "{ "+upperKey+" }", value)

		// 3. Square brackets: [code] or [CODE]
		result = strings.ReplaceAll(result, "["+key+"]", value)
		result = strings.ReplaceAll(result, "["+upperKey+"]", value)
		result = strings.ReplaceAll(result, "[ "+key+" ]", value)
		result = strings.ReplaceAll(result, "[ "+upperKey+" ]", value)
	}

	return result
}
