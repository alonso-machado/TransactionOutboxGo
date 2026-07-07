// Test 6.2 — autoscaling: floods ONE shard's order queue past KEDA's
// queueLengthValue trigger (1000, see helmcharts/transaction-outbox
// values.yaml orderConsumerWorker.keda) to force scale-up, then stops
// producing so the queue drains and KEDA scales order-consumer-worker back
// to 0.
//
// order-consumer-worker instances must NOT be pinned for this test (the
// opposite of 6.1) — run against the real KEDA config: minReplicaCount 0,
// maxReplicaCount 10. The scaling itself is observed out-of-band via
// `kubectl get pods/scaledobject` — k6 only produces the load.
//
// Closed-loop hammer (constant-vus), not a capped open-loop rate: a fixed
// target rate (the original design) self-limits at whatever 1 consumer can
// already drain, so the backlog never climbs past KEDA's threshold long
// enough to justify a 2nd replica. VUS VUs loop "send, get response,
// immediately send again" with no artificial ceiling, so throughput is
// whatever the system can sustain, not a number picked in advance.
//
// Kubernetes-only (KEDA needs K8s) — not run against local compose.
import http from "k6/http";
import { check } from "k6";
import { buildBody, SHARDS } from "./payloads.js";

// EVENT_TYPE/EVENT_SUBTYPE default to CONCERT/ROCK — the first entry in
// SHARDS, matching payloads.js and the shard docker-compose.yml/values.yaml
// both run by default.
const EVENT_TYPE = __ENV.EVENT_TYPE || SHARDS[0].eventType;
const EVENT_SUBTYPE = __ENV.EVENT_SUBTYPE || SHARDS[0].eventSubtype;
const SHARD = { eventType: EVENT_TYPE, eventSubtype: EVENT_SUBTYPE };
const BASE = __ENV.TARGET_URL || "http://localhost:8080";
const VUS = Number(__ENV.VUS || 200);
const DURATION = __ENV.DURATION || "8m";

export const options = {
  scenarios: {
    flood: {
      executor: "constant-vus",
      vus: VUS,
      duration: DURATION,
    },
  },
};

export default function () {
  const body = buildBody(SHARD);
  const res = http.post(`${BASE}/api/v1/orders`, JSON.stringify(body), {
    headers: {
      "Content-Type": "application/json",
      "Idempotency-Key": body.__idempotencyKey,
    },
    tags: { eventType: EVENT_TYPE, eventSubtype: EVENT_SUBTYPE },
  });
  check(res, { "is 201": (r) => r.status === 201 });
}
