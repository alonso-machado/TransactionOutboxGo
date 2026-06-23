// Test 6.3 — consumer-worker in isolation: publishes straight onto a
// per-method RabbitMQ queue (bypassing ingestion-api/DispatchOutbox
// entirely). k6 itself only does the AMQP publish side and reports its own
// publish throughput/latency (k6-native metrics) — consumer-worker's own
// behavior (ack / duplicate / retry_scheduled / poison_dlq /
// unknown_schema_version) is read from ITS Prometheus /metrics endpoint
// (consumer_messages_processed_total{outcome=...}), not from k6. Snapshot
// that endpoint before and after the run and diff the counters — see
// loadtest/README.md's "Checking consumer behavior" section for the exact
// commands.
//
// Needs the custom k6 binary from build/k6/Dockerfile (xk6-amqp).
//
// Every run is a MIX of three message shapes by default, so a single
// invocation naturally exercises all of the consumer's outcomes in one
// pass (mirrors how redeliveries/bad-version messages actually show up in
// production — mixed in with everything else, not as a separate batch):
//   - the rest                  — unique, well-formed -> outcome=ack
//   - DUP_FRACTION (10%)        — reuses a prior iteration's identity
//                                 (MessageId/eventId) -> outcome=duplicate
//   - SCHEMA_FRACTION (2%)      — carries an unrecognized schemaVersion,
//                                 rejected straight to DLQ on the first
//                                 attempt -> outcome=unknown_schema_version
// Set either fraction to 0 to exclude that behavior from a run.
import amqp from "k6/x/amqp";
import { Counter } from "k6/metrics";

const METHOD = __ENV.METHOD || "PIX";
const N = Number(__ENV.N || 10000);
const DUP_FRACTION = Number(__ENV.DUP_FRACTION || 0.1);
const SCHEMA_FRACTION = Number(__ENV.SCHEMA_FRACTION || 0.02);
const RABBITMQ_URL = __ENV.RABBITMQ_URL || "amqp://guest:guest@localhost:5672/";

amqp.start({ connection_url: RABBITMQ_URL });

const messagesPublished = new Counter("messages_published");
const duplicatesPublished = new Counter("duplicates_published");
const badSchemaPublished = new Counter("bad_schema_published");

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

// Wire shape matching internal/usecase/consume's payloadDTO (what
// DispatchOutbox's publisher actually puts on the wire) — top-level fields,
// not nested under "payment" like the HTTP ingest DTO. See
// tests/integration/consumer_test.go's consumerPayload for the reference.
function buildOutboxPayload(method, paymentId, eventId, schemaVersion) {
  return {
    schemaVersion,
    paymentId,
    eventId,
    providerName: "LOADTEST",
    providerPaymentId: `prov-${eventId}`,
    externalPaymentId: `pay-${eventId}`,
    amount: 10050,
    currency: "BRL",
    method,
    methodDetails: methodDetailsFor(method),
    occurredAt: new Date().toISOString(),
  };
}

function methodDetailsFor(method) {
  switch (method) {
    case "PIX":
      return { endToEndId: "E00000000000000000000000000", txid: "ORDER-LOADTEST" };
    case "BOLETO":
      return { barcode: "00000000000000000000000000000000000000000000", dueDate: "2026-12-31", payerDocument: "00000000000" };
    case "CARTAO_CREDITO":
      return { cardNumber: "************1111", cardType: "CREDIT", cardIssuer: "VISA" };
    case "CARTAO_DEBITO":
      return { cardNumber: "************1111", cardType: "DEBIT", cardIssuer: "MASTERCARD" };
    default:
      return {};
  }
}

function uuidv4Like(seed) {
  // Deterministic-enough fake UUID for load purposes — the consumer only
  // requires a parseable UUID, not a cryptographically unique one.
  const h = `${seed}`.padStart(12, "0").slice(-12);
  return `00000000-0000-7000-8000-${h}`;
}

export default function () {
  const seed = `${__VU}-${__ITER}-${Date.now()}`;
  const paymentId = uuidv4Like(__ITER);
  let eventId = `evt-loadtest-${seed}`;
  let messageId = `msgid-loadtest-${seed}`;
  let schemaVersion = "1";

  if (DUP_FRACTION > 0 && Math.random() < DUP_FRACTION) {
    // Reuse a previous iteration's identity so the consumer's
    // (source_message_id, occurred_at) dedup is exercised -> outcome=duplicate.
    const dupOf = Math.max(0, __ITER - 1);
    eventId = `evt-loadtest-dup-${dupOf}`;
    messageId = `msgid-loadtest-dup-${dupOf}`;
    duplicatesPublished.add(1);
  } else if (SCHEMA_FRACTION > 0 && Math.random() < SCHEMA_FRACTION) {
    // An unrecognized schemaVersion can never succeed on retry, so the
    // consumer rejects it straight to DLQ on the first attempt ->
    // outcome=unknown_schema_version.
    schemaVersion = "999";
    badSchemaPublished.add(1);
  }

  const body = buildOutboxPayload(METHOD, paymentId, eventId, schemaVersion);
  amqp.publish({
    exchange: "payments.exchange",
    // xk6-amqp's PublishOptions has no routing_key field — QueueName is
    // passed straight through as the 2nd ("key") argument to the
    // underlying amqp091-go Publish(exchange, key, ...) call, so it IS the
    // routing key whenever Exchange is set (the field name only means
    // literally "queue name" in the README's default-exchange example).
    // Using routing_key here (as a first attempt did) is silently ignored
    // -> empty routing key -> a topic exchange matches no binding -> the
    // message vanishes with no error and no queue growth.
    queue_name: `payment.${METHOD.toLowerCase()}`,
    message_id: messageId,
    content_type: "application/json",
    body: JSON.stringify(body),
    persistent: true,
  });
  messagesPublished.add(1);
}
