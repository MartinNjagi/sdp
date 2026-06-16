package worker

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"sdp/controllers/mno_router"
	"sdp/controllers/publisher"
	"sdp/controllers/storage" // Injected S3/Minio Service
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
// For each envelope it:
//  1. Fetches the contact list from either S3/Minio (CSV) OR the Database.
//  2. Parses each row/record and compiles the message (template + replacements).
//  3. Resolves the destination carrier and prices the message in credits.
//  4. Writes an Outbox row per contact.
//  5. Fan-out publishes a DispatchEnvelope per contact to the standard queue.
type BulkWorker struct {
	ctx        context.Context
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
	conn *amqplib.Connection,
	pub *publisher.Publisher,
	router *mno_router.Router,
	costEngine *data.CostEngine,
	db *gorm.DB,
	s3Svc *storage.S3Service,
	poolSize int, s3Bucket string,
) (*BulkWorker, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("bulk worker: open channel: %w", err)
	}

	// Bulk messages are heavy — fetch only one at a time per goroutine.
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

func (w *BulkWorker) processContact(env data.BulkEnvelope, contact Contact) error {
	compiled := compileTemplate(env.TemplateID, contact.Replacements)

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

	outboxID, err := w.writeOutbox(env, contact.MSISDN, compiled, priced.TotalCredits)
	if err != nil {
		return fmt.Errorf("write outbox: %w", err)
	}

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
	MSISDN       string
	Replacements map[string]string // Key → value for template substitution
}

// fetchContactsFromDB queries the normalized address book tables for all
// active members of a contact group. It automatically drops blacklisted numbers.
func (w *BulkWorker) fetchContactsFromDB(groupID uint64) ([]Contact, error) {
	// Struct to capture the specific projection from our JOIN query
	type addressBookEntry struct {
		MSISDN      string
		ContactName string
	}

	var entries []addressBookEntry

	// Perform the multi-table JOIN:
	// contact_group_members -> client_address_books -> phone_numbers
	err := w.db.Table("contact_group_members AS cgm").
		Select("pn.msisdn, cab.contact_name").
		Joins("JOIN client_address_books AS cab ON cgm.client_id = cab.client_id AND cgm.phone_id = cab.phone_id").
		Joins("JOIN phone_numbers AS pn ON cab.phone_id = pn.id").
		Where("cgm.group_id = ?", groupID).
		Where("cab.is_blacklisted = ?", false). // Automatically drop opted-out/blacklisted numbers
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
			continue // Failsafe
		}

		// Map the fields to template replacements.
		// We add a few variations so users can use {{name}} or {{contact_name}} in templates.
		replacements := map[string]string{
			"msisdn":       msisdn,
			"contact_name": entry.ContactName,
			"name":         entry.ContactName,
		}

		contacts = append(contacts, Contact{
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

// compileTemplate performs {{key}} substitution on the template body.
func compileTemplate(template string, replacements map[string]string) string {
	result := template
	for key, value := range replacements {
		result = strings.ReplaceAll(result, "{{"+key+"}}", value)
		result = strings.ReplaceAll(result, "{{ "+key+" }}", value)
	}
	return result
}
