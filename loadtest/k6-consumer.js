// Test 6.3 — consumer-worker in isolation: publishes straight onto a
// per-method RabbitMQ queue (bypassing ingestion-api/DispatchOutbox
// entirely) and polls Postgres to measure the consumer's own drain rate and
// consume->persist latency, DB write included.
//
// Needs the custom k6 binary built from build/k6/Dockerfile (xk6-amqp +
// xk6-sql). Two scenarios, selected via __ENV.SCENARIO:
//   drain  (default) — publish N unique messages, measure persisted/sec.
//   dedup             — publish N messages where a DUP_FRACTION carry
//                        duplicate MessageId/source_message_id, assert the
//                        final payments count == distinct keys published.
//
// Points at a load/test database only — PGDATABASE must not be the prod
// name (see README.md). The dedup scenario does NOT truncate automatically;
// truncate the test DB yourself between runs if rerunning the same scenario.
import amqp from "k6/x/amqp";
import sql from "k6/x/sql";
import { Trend, Counter } from "k6/metrics";

const persistLatency = new Trend("consume_to_persist_latency", true);
const persisted = new Counter("messages_persisted");
const dedupCollisions = new Counter("dedup_collisions");

const METHOD = __ENV.METHOD || "PIX";
const N = Number(__ENV.N || 10000);
const SCENARIO = __ENV.SCENARIO || "drain"; // "drain" | "dedup"
const DUP_FRACTION = Number(__ENV.DUP_FRACTION || 0.1);
const RABBITMQ_URL = __ENV.RABBITMQ_URL || "amqp://guest:guest@localhost:5672/";
const DATABASE_URL = __ENV.DATABASE_URL || "postgres://outbox:outbox@localhost:5432/outbox?sslmode=disable";

const db = sql.open("postgres", DATABASE_URL);
amqp.start({ connection_url: RABBITMQ_URL });

export const options = {
  scenarios: {
    publish: {
      executor: "shared-iterations",
      vus: 50,
      iterations: N,
      maxDuration: "30m",
    },
  },
};

// Wire shape matching internal/usecase/consume's payloadDTO (what
// DispatchOutbox's publisher actually puts on the wire) — top-level fields,
// not nested under "payment" like the HTTP ingest DTO. See
// tests/integration/consumer_test.go's consumerPayload for the reference.
function buildOutboxPayload(method, paymentId, eventId) {
  return {
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

  if (SCENARIO === "dedup" && Math.random() < DUP_FRACTION) {
    // Reuse a previous iteration's identity so the consumer's
    // ON CONFLICT (source_message_id) DO NOTHING dedup is exercised.
    const dupOf = Math.max(0, __ITER - 1);
    eventId = `evt-loadtest-dup-${dupOf}`;
    messageId = `msgid-loadtest-dup-${dupOf}`;
  }

  const body = buildOutboxPayload(METHOD, paymentId, eventId);
  const publishStart = Date.now();
  amqp.publish({
    exchange: "payments.exchange",
    routing_key: `payment.${METHOD.toLowerCase()}`,
    message_id: messageId,
    body: JSON.stringify(body),
    persistent: true,
  });
  body.__publishStart = publishStart;
}

export function teardown() {
  // Poll until the batch is drained (or timeout), then report throughput
  // and consume->persist latency for a sample of rows.
  const deadline = Date.now() + 5 * 60 * 1000;
  let count = 0;
  while (Date.now() < deadline) {
    const rows = sql.query(db, "SELECT count(*) AS n FROM payments");
    count = rows[0] ? Number(rows[0].n) : 0;
    if (SCENARIO === "dedup") {
      const expected = Math.ceil(N * (1 - DUP_FRACTION));
      if (count >= expected) break;
    } else if (count >= N) {
      break;
    }
  }
  persisted.add(count);

  if (SCENARIO === "dedup") {
    const dupCount = N - Math.ceil(N * (1 - DUP_FRACTION));
    dedupCollisions.add(dupCount);
  }

  const latencyRows = sql.query(
    db,
    "SELECT extract(epoch from (now() - created_at)) AS age_seconds FROM payments ORDER BY created_at DESC LIMIT 1000"
  );
  for (const row of latencyRows) {
    persistLatency.add(Number(row.age_seconds) * 1000);
  }

  db.close();
}
