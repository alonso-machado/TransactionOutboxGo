// Test 6.3 — order-consumer-worker in isolation: publishes straight onto
// one or more shards' order_outbox queues (bypassing ingestion-api/
// DispatchOutbox entirely). k6 itself only does the AMQP publish side and
// reports its own publish throughput/latency (k6-native metrics) —
// order-consumer-worker's own behavior (ack / duplicate / retry_scheduled /
// poison_dlq / unknown_schema_version) is read from ITS Prometheus /metrics
// endpoint (consumer_messages_processed_total{outcome=...}), not from k6.
// Snapshot that endpoint before and after the run and diff the counters —
// see loadtest/README.md's "Checking consumer behavior" section for the
// exact commands.
//
// Scoped to the order stream only (not payment_event_outbox/fulfillment-
// consumer-worker): fulfillment-consumer-worker's IssueTickets looks up a
// real Charge row by provider_ref, so a directly-published payment-event
// message with no matching Charge would just error on every single
// message — meaningfully load-testing that path needs real Charges seeded
// first, out of scope here.
//
// Needs the custom k6 binary from build/k6/Dockerfile (xk6-amqp).
//
// SHARDS (__ENV.SHARDS) is a comma-separated list of "eventType:eventSubtype"
// pairs, e.g. "CONCERT:ROCK,SPORTS:FOOTBALL" — iterations round-robin across
// it, splitting N evenly. Useful for comparing two shards' consumer/DB
// throughput side by side (see loadtest/README.md's 6.3 results for a worked
// example).
//
// Every run is a MIX of three message shapes by default, so a single
// invocation naturally exercises all of the consumer's outcomes in one
// pass (mirrors how redeliveries/bad-version messages actually show up in
// production — mixed in with everything else, not as a separate batch):
//   - the rest                  — unique, well-formed -> outcome=ack
//   - DUP_FRACTION (10%)        — reuses a prior iteration's identity
//                                 (MessageId/sourceOrderId) -> outcome=duplicate
//   - SCHEMA_FRACTION (2%)      — carries an unrecognized schemaVersion,
//                                 rejected straight to DLQ on the first
//                                 attempt -> outcome=unknown_schema_version
// Set either fraction to 0 to exclude that behavior from a run.
import amqp from "k6/x/amqp";
import { Counter } from "k6/metrics";

const SHARDS = (__ENV.SHARDS || __ENV.SHARD || "CONCERT:ROCK")
  .split(",")
  .map((s) => {
    const [eventType, eventSubtype] = s.trim().split(":");
    return { eventType: eventType.toUpperCase(), eventSubtype: (eventSubtype || "").toUpperCase() };
  })
  .filter((s) => s.eventType && s.eventSubtype);
const N = Number(__ENV.N || 10000);
const DUP_FRACTION = Number(__ENV.DUP_FRACTION || 0.1);
const SCHEMA_FRACTION = Number(__ENV.SCHEMA_FRACTION || 0.02);
const RABBITMQ_URL = __ENV.RABBITMQ_URL || "amqp://guest:guest@localhost:5672/";

const messagesPublished = new Counter("messages_published");
const duplicatesPublished = new Counter("duplicates_published");
const badSchemaPublished = new Counter("bad_schema_published");
// Per-shard publish counters (e.g. messages_published_concert_rock,
// messages_published_sports_football) so the k6 summary itself shows the
// split without having to cross-reference RabbitMQ/Prometheus for it.
const perShardPublished = {};
for (const s of SHARDS) {
  perShardPublished[shardKey(s)] = new Counter(`messages_published_${shardKey(s)}`);
}

function shardKey(s) {
  return `${s.eventType}_${s.eventSubtype}`.toLowerCase();
}

export const options = {
  scenarios: {
    publish: {
      executor: "shared-iterations",
      vus: 100,
      iterations: N,
      maxDuration: "30m",
    },
  },
};

// Wire shape matching internal/usecase/checkout's payloadDTO (what
// DispatchOutbox's publisher actually puts on the wire for order_outbox) —
// see tests/integration/checkout_test.go's orderOutboxPayload for the
// reference.
function buildOrderOutboxPayload(shard, orderId, sourceOrderId, sourceEventId, schemaVersion) {
  return {
    schemaVersion,
    orderId,
    sourceOrderId,
    eventType: shard.eventType,
    eventSubtype: shard.eventSubtype,
    sourceEventId,
    eventName: "LOADTEST",
    sourceVenueId: "venue-loadtest",
    venueName: "LOADTEST Arena",
    venueCity: "LOADTEST City",
    items: [{ sourceTicketId: `TKT-${sourceOrderId}`, section: "A", row: "1", seat: "1", price: 10050, currency: "BRL" }],
    customerName: "LOADTEST",
    customerEmail: "loadtest@example.com",
    customerDocument: "00000000000",
    amount: 10050,
    currency: "BRL",
  };
}

function uuidv4Like(seed) {
  // Deterministic-enough fake UUID for load purposes — the consumer only
  // requires a parseable UUID, not a cryptographically unique one.
  const h = `${seed}`.padStart(12, "0").slice(-12);
  return `00000000-0000-7000-8000-${h}`;
}

// setup() runs once, single-threaded, before any VU starts — calling
// amqp.start() here (instead of once per VU) avoids a real bug in
// xk6-amqp: its connection registry is an unsynchronized map, and 100 VUs
// all calling start() concurrently at VU-init hits a "fatal error:
// concurrent map writes" that crashes the whole process. Every VU below
// reuses this one connection via connection_id.
export function setup() {
  const connectionId = amqp.start({ connection_url: RABBITMQ_URL });
  return { connectionId };
}

export default function (data) {
  // Round-robin across SHARDS — e.g. with "CONCERT:ROCK,SPORTS:FOOTBALL",
  // even iterations go to CONCERT/ROCK and odd ones to SPORTS/FOOTBALL,
  // splitting N evenly between them.
  const shard = SHARDS[__ITER % SHARDS.length];

  const seed = `${__VU}-${__ITER}-${Date.now()}`;
  const orderId = uuidv4Like(__ITER);
  let sourceOrderId = `order-loadtest-${seed}`;
  let sourceEventId = `evt-loadtest-${seed}`;
  let messageId = `msgid-loadtest-${seed}`;
  let schemaVersion = "1";

  if (DUP_FRACTION > 0 && Math.random() < DUP_FRACTION) {
    // Reuse a prior iteration's identity to exercise the consumer's
    // orders.source_order_id dedup -> outcome=duplicate. Must be an
    // iteration that published the SAME shard — stepping back by
    // SHARDS.length lands on the previous iteration that shared this same
    // round-robin slot.
    const dupOf = Math.max(0, __ITER - SHARDS.length);
    sourceOrderId = `order-loadtest-dup-${dupOf}`;
    sourceEventId = `evt-loadtest-dup-${dupOf}`;
    messageId = `msgid-loadtest-dup-${dupOf}`;
    duplicatesPublished.add(1);
  } else if (SCHEMA_FRACTION > 0 && Math.random() < SCHEMA_FRACTION) {
    // An unrecognized schemaVersion can never succeed on retry, so the
    // consumer rejects it straight to DLQ on the first attempt ->
    // outcome=unknown_schema_version.
    schemaVersion = "999";
    badSchemaPublished.add(1);
  }

  const body = buildOrderOutboxPayload(shard, orderId, sourceOrderId, sourceEventId, schemaVersion);
  amqp.publish({
    connection_id: data.connectionId,
    exchange: "tickets.exchange",
    // xk6-amqp's PublishOptions has no routing_key field — QueueName is
    // passed straight through as the 2nd ("key") argument to the
    // underlying amqp091-go Publish(exchange, key, ...) call, so it IS the
    // routing key whenever Exchange is set (the field name only means
    // literally "queue name" in the README's default-exchange example).
    // Using routing_key here (as a first attempt did) is silently ignored
    // -> empty routing key -> a topic exchange matches no binding -> the
    // message vanishes with no error and no queue growth.
    queue_name: `order.${shard.eventType.toLowerCase()}.${shard.eventSubtype.toLowerCase()}`,
    message_id: messageId,
    content_type: "application/json",
    body: JSON.stringify(body),
    persistent: true,
  });
  messagesPublished.add(1);
  perShardPublished[shardKey(shard)].add(1);
}
