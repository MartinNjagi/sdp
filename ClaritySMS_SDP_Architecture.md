# ClaritySMS Service Delivery Platform
**Engineering Reference Document**
*Prepared for the ClaritySMS Engineering Team*

## Table of Contents
1. [System Overview](#1-system-overview)
    - [1.1 Design Principles](#11-design-principles)
2. [Ingestion](#2-ingestion)
    - [2.1 Three Queues, One Exchange](#21-three-queues-one-exchange)
    - [2.2 Envelope Shapes](#22-envelope-shapes)
3. [Processing](#3-processing)
    - [3.1 Bulk Fan-Out](#31-bulk-fan-out)
    - [3.2 The Dispatch Pipeline](#32-the-dispatch-pipeline)
    - [3.3 Hot Wallet Mechanics](#33-hot-wallet-mechanics)
    - [3.4 Failure Handling & Refunds](#34-failure-handling--refunds)
4. [Dispatch to Mobile Network Operators](#4-dispatch-to-mobile-network-operators)
5. [Delivery Receipt Reconciliation](#5-delivery-receipt-reconciliation)
6. [End-to-End Trace](#6-end-to-end-trace)
7. [Unfinished & Open Items](#7-unfinished--open-items)

---

## 1. System Overview
The Service Delivery Platform (SDP) is the gateway layer bridging the Core Engine to Mobile Network Operators (MNOs) — Safaricom, Airtel, and Africa's Talking.
It owns message queuing, traffic shaping, protocol dispatch, hot-wallet credit deductions, and delivery receipt (DLR) reconciliation.

The platform is organised into three operational zones, each independently scalable:
* **Zone 1 — Client Layer:** human interaction, CSV uploads, API key management (outside this document's scope).
* **Zone 2 — Core Engine:** authentication, the cold DB ledger, compliance, and campaign scheduling.
* **Zone 3 — SDP Gateway:** everything documented here — queuing, hot wallet, routing, dispatch, and DLR ingestion.

### 1.1 Design Principles
* **Dependency injection throughout** — every component receives only what it needs via constructor parameters or a Deps bundle, with no global state or package-level variables.
* **Three isolated message lanes** (VIP, Standard, Bulk) so a large campaign blast can never delay a time-critical OTP.
* **Atomic financial operations** — every credit deduction and refund runs as a single Lua script in Redis, eliminating race conditions even at high throughput.
* **Idempotent topology** — RabbitMQ exchanges and queues are declared on every startup; rerunning a deploy never duplicates or corrupts the broker state.

---

## 2. Ingestion
Messages enter the SDP through the Publisher, which exposes typed methods for each message class.
The Core Engine never constructs raw queue payloads — it calls a method and the Publisher handles routing key selection, JSON encoding, and persistence flags.

### 2.1 Three Queues, One Exchange
All three queues bind to a single direct exchange (`sms.outbound`) and share one dead-letter exchange.
Messages that exceed their TTL or exhaust all retries land in the dead-letter queue for manual inspection rather than disappearing silently.

| Queue                          | Routing Key              | Used For                                | Pool Size |
|--------------------------------|--------------------------|-----------------------------------------|-----------|
| `sms.q.transactional.vip`      | `transactional.vip`      | OTPs, auth codes, password resets       | 10        |
| `sms.q.transactional.standard` | `transactional.standard` | Receipts, notifications, single sends   | 5         |
| `sms.q.bulk.campaigns`         | `bulk.campaigns`         | Campaign launch envelopes (pre fan-out) | 2         |

### 2.2 Envelope Shapes
Three distinct envelope types exist because bulk and transactional messages carry very different amounts of pre-computed data:
* **BulkEnvelope** — lean. Carries only `campaign_id`, `client_id`, `template_id`, `sender_id`, `contact_group_id`, and an S3/Minio `file_url`. No per-recipient data is included; that is reconstructed downstream.
* **TransactionalEnvelope** — near-complete. Carries the destination MSISDN, template, message body, a replacements map, and a priority flag ("high" routes to VIP).
* **DispatchEnvelope** — terminal and fully compiled. The only shape the dispatch layer ever sees: `outbox_id`, `msisdn`, `sender_id`, `message`, `message_type`, an integer credit cost, and a retry count.

---

## 3. Processing

### 3.1 Bulk Fan-Out
The `BulkWorker` is a pure expander — it never talks to an MNO.
For every `BulkEnvelope` it consumes, it performs four steps in sequence:
1. Download the contact CSV from the `file_url` over HTTP.
2. Parse each row into an MSISDN plus a map of column-to-value replacements.
3. Compile the template by substituting `{{placeholder}}` tokens.
4. Write one Outbox row per contact before publishing a `DispatchEnvelope` for each onto the standard queue.

Partial failures (a handful of malformed rows) are logged and skipped rather than failing the entire campaign.

### 3.2 The Dispatch Pipeline
Every `DispatchEnvelope`, whether produced by fan-out or published directly for a transactional send, passes through the same six-stage pipeline inside a `DispatchWorker`:
1. **Decode** — unmarshal the JSON envelope; malformed payloads are dead-lettered immediately.
2. **Wallet deduction** — an atomic Lua script checks the client's hot balance and deducts the message cost in one Redis round-trip. Insufficient credit dead-letters the message without ever reaching the network.
3. **Routing** — the MNO Router resolves the destination MSISDN to a carrier using longest-prefix matching against a configured routing table.
4. **Rate limiting** — a per-MNO token bucket blocks the goroutine until a TPS slot is free, guaranteeing the platform never exceeds a carrier's contracted bind limit.
5. **Dispatch** — the resolved Dispatcher (Africa's Talking or Safaricom SDP) sends the message over HTTP and returns a provider message ID.
6. **DB update & acknowledgement** — the Outbox row is marked SENT with the provider ID for later DLR matching, and the AMQP delivery is acknowledged.

### 3.3 Hot Wallet Mechanics
Balances are integer message credits, not currency — one credit roughly equals one billable SMS unit.
Redis holds the live balance plus a per-client pending-deduction accumulator.

A background Flusher drains every active client's accumulator on a fixed interval (default 30 seconds) and ships a single batched HTTP POST to the Core Engine's cold ledger, so the PostgreSQL wallet table never receives more than one write per client per flush window even under a large campaign blast.

If a client has no cached balance — a cold Redis instance or a brand-new client — the wallet falls back to a synchronous call against the Core Engine's internal balance endpoint, seeds the result into Redis, and proceeds. This call uses an integer-only contract: a `ClientID` string in, and a `ClientID`/`Balance`/`Currency` triple back, where `Balance` is always a whole-number credit count.

### 3.4 Failure Handling & Refunds
Dispatch errors are classified as Temporary or Permanent by each Dispatcher implementation.
* **Temporary errors** (provider 5xx, network timeouts, HTTP 429) are republished with exponential backoff up to three attempts.
* **Permanent errors** (invalid number, rejected sender ID, malformed payload) dead-letter immediately.

In both the no-route and permanent-failure cases, any credit already deducted in step 2 is refunded atomically before the message is given up on, so a client is never charged for a message that never reached a carrier.

---

## 4. Dispatch to Mobile Network Operators
Two HTTP-based dispatchers are implemented today, selected per-route by the MNO Router's configuration.

| Dispatcher       | Provider         | Endpoint(s)                                                               | Notes                                                                                                         |
|------------------|------------------|---------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------|
| `http_at`        | Africa's Talking | `POST /version1/messaging`                                                | Single endpoint; status 100 = success.                                                                        |
| `http_safaricom` | Safaricom SDP    | `POST /CMS/bulksms` (bulk),<br>`POST /SDP/sendSMSRequest` (transactional) | Routes by `message_type`; bulk has a fallback URL on network failure; 429 and status-0 responses are retried. |

The Safaricom dispatcher reads its Bearer token from Redis via an injected closure rather than managing auth itself — token refresh is owned by a separate process, keeping the dispatcher's only responsibility the HTTP exchange.

---

## 5. Delivery Receipt Reconciliation
MNOs report final delivery status asynchronously, days after the original Send call returns.
The Reconciler is the single point where this status is normalised and acted upon.
* **Ingestion** — a Gin webhook handler (`POST /webhooks/dlr/at` for Africa's Talking, plus a generic JSON handler for new integrations) translates the carrier-specific payload into a `RawDLR` struct.
* **Normalisation** — carrier-native codes (DELIVRD, 000, REJECTD, UNDELIV, and so on) map to one of three platform statuses: DELIVERED, FAILED, or REJECTED.
* **Idempotency guard** — if the Outbox row is already in a terminal state, the reconciler exits without rewriting it, protecting against duplicate DLR deliveries from the carrier.
* **Wallet refund** — on FAILED status, the `HotWallet`'s Refund method credits the client back.
* **Live dashboard push** — the result is broadcast over Redis Pub/Sub to the same channel (`ws:messages`) that campaign progress events use, so the Node BFF's existing subscriber needs no additional wiring to surface DLR updates in real time.

---

## 6. End-to-End Trace
A condensed trace of a single transactional OTP message, from API call to delivered handset:
1. Core Engine calls `publisher.PublishVIP()` with a `TransactionalEnvelope`.
2. Message lands on `sms.q.transactional.vip`.
3. A VIP `DispatchWorker` goroutine dequeues it.
4. `HotWallet.Deduct()` atomically removes 1 credit from the client's Redis balance.
5. MNO Router resolves the MSISDN prefix to "safaricom".
6. Rate limiter waits for a free TPS slot on the safaricom bucket.
7. `SafaricomDispatcher.Send()` POSTs to `/SDP/sendSMSRequest`.
8. Outbox row is updated to SENT with the provider's message ID.
9. Minutes later, Safaricom POSTs a DLR webhook with status DELIVRD.
10. Reconciler normalises it to DELIVERED, updates the Outbox row, and publishes to `ws:messages`.
11. The Node BFF's existing SSE subscriber pushes the update to the client's dashboard.

---

## 7. Unfinished & Open Items
The platform is functional end-to-end for HTTP-based transactional and bulk traffic.
The following items are explicitly incomplete and should not be assumed to work in production:

### 7.1 SMPP Transceiver
* **Status:** Stub only. `SMPPDispatcher.Send()` always returns a permanent error. No TCP bind, no `EnquireLink` heartbeat, no PDU windowing exists yet. Any route configured with `dispatcher: "smpp"` will dead-letter every message immediately.

### 7.2 Client Webhook Firer
* **Status:** Interface defined, not implemented. The Reconciler's `WithWebhookFirer` hook exists and is ready to accept an implementation, but nothing currently calls it from `sdp.go`. Client-configured outbound DLR webhooks will not fire until this is wired up.

### 7.3 Hot Wallet Seeding on Startup
* **Status:** Reactive only. The wallet currently seeds a client's Redis balance lazily, on first read after a cache miss. There is no proactive warm-up job that pre-loads active clients' balances when the SDP boots, so the very first message per client after a Redis restart always pays the latency cost of a synchronous Core Engine round-trip.

### 7.4 Mobile Originated (MO) / Two-Way Messaging
* **Status:** Not started. Inbound SMS routing (shortcode replies, auto-responders) was scoped in the original blueprint as an optional future flow and has no corresponding code in any package.

---
*End of document.*
