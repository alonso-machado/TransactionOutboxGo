// Body builders for ticket orders, matching the wire format
// internal/adapter/http/order_dto.go expects. Each call produces a body with
// a fresh sourceOrderId/eventId/ticket id and an Idempotency-Key — without
// that, dedup would collapse every iteration into a single order_outbox row
// and the load test would measure nothing.
//
// customer.name is always "LOADTEST" — the marker cmd/outbox-admin's
// purge-loadtest-dlq matches on for the order stream (see its
// isLoadtestMessage), so a DLQ full of a mix of real and loadtest orders can
// be cleaned safely.

// SHARDS mirrors the two (event_type, event_subtype) pairs docker-compose.yml
// runs order-consumer-worker/fulfillment-consumer-worker instances for by
// default — see internal/infrastructure/rabbitmq.EventTypes for the full
// registry.
export const SHARDS = [
  { eventType: "CONCERT", eventSubtype: "ROCK" },
  { eventType: "SPORTS", eventSubtype: "FOOTBALL" },
];

let counter = 0;

function uniqueSuffix() {
  counter += 1;
  return `${Date.now()}-${__VU}-${__ITER}-${counter}`;
}

function order(shard, suffix) {
  const body = {
    sourceOrderId: `order-loadtest-${suffix}`,
    eventType: shard.eventType,
    eventSubtype: shard.eventSubtype,
    eventId: `evt-loadtest-${suffix}`,
    eventName: "LOADTEST",
    venue: { id: "venue-loadtest", name: "LOADTEST Arena", city: "LOADTEST City" },
    tickets: [
      { id: `TKT-loadtest-${suffix}`, section: "A", row: "1", seat: "1", price: 100.5, currency: "BRL" },
    ],
    customer: { name: "LOADTEST", email: "loadtest@example.com", document: "00000000000" },
  };
  body.__idempotencyKey = `loadtest-${suffix}`;
  return body;
}

// buildBody returns a fresh, valid order body for shard (an entry from
// SHARDS), tagged with a unique sourceOrderId/Idempotency-Key
// (body.__idempotencyKey — strip before sending if the target doesn't
// tolerate unknown fields; the ingestion-api DTO ignores unrecognized
// top-level keys so this is safe to send as-is).
export function buildBody(shard) {
  return order(shard, uniqueSuffix());
}
