package data

const DESCRIPTION = "description"
const FrontEndTimeFormat = "2006-01-02 15:04" // yyyy-MM-dd HH:mm

const (
	// OutboundExchange is the durable direct exchange for MT messages.
	OutboundExchange = "sms.outbound"

	// OutboundQueue is the durable queue workers consume from.
	OutboundQueue = "sms.outbound.q"

	// OutboundRoutingKey is the binding key between exchange and queue.
	OutboundRoutingKey = "outbound"

	// DeadLetterExchange receives messages that exhaust all retries.
	DeadLetterExchange = "sms.dead"

	// DeadLetterQueue holds permanently failed messages for inspection.
	DeadLetterQueue = "sms.dead.q"

	// MessageTTL is the maximum time (ms) a message may sit undelivered in the queue.
	// 24 hours — gives the platform time to recover from a full Telco outage.
	MessageTTL = 86_400_000
)

const (
	// maxRetries is the number of times a message is requeued on a temporary
	// error before being dead-lettered.
	maxRetries = 3

	// prefetchCount controls how many unacknowledged messages the broker pushes
	// to this worker at once. Keeps memory bounded and prevents one slow MNO
	// from starving others.
	prefetchCount = 50
)
