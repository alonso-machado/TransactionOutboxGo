// Test 6.1 — two-phase latency baseline (P95/P99), consumers PINNED at 1
// replica/shard (KEDA disabled or paused). See loadtest/README.md.
//
// Phase A (5 min): 100 VUs round-robin across all shards (SHARDS in
// payloads.js).
// Phase B (5 min): 100 VUs hit CONCERT/ROCK only, to contrast a single hot
// queue against the spread-out mixed load and show the single-consumer
// drain ceiling building backlog.
import http from "k6/http";
import { check } from "k6";
import { buildBody, SHARDS } from "./payloads.js";

export const options = {
  scenarios: {
    mixed: {
      executor: "constant-vus",
      vus: 100,
      duration: "5m",
      exec: "mixed",
      startTime: "0s",
    },
    oneShardOnly: {
      executor: "constant-vus",
      vus: 100,
      duration: "5m",
      exec: "oneShardOnly",
      startTime: "5m",
    },
  },
  thresholds: {
    // Placeholders — calibrate after the first real run against a real target.
    "http_req_duration": ["p(95)<500", "p(99)<1000"],
    "http_req_failed": ["rate<0.01"],
  },
};

const BASE = __ENV.TARGET_URL || "http://localhost:8080";

function post(shard) {
  const body = buildBody(shard);
  const res = http.post(`${BASE}/api/v1/orders`, JSON.stringify(body), {
    headers: {
      "Content-Type": "application/json",
      "Idempotency-Key": body.__idempotencyKey,
    },
    tags: { eventType: shard.eventType, eventSubtype: shard.eventSubtype },
  });
  check(res, { "is 201": (r) => r.status === 201 });
}

export function mixed() {
  post(SHARDS[__ITER % SHARDS.length]);
}

export function oneShardOnly() {
  post(SHARDS[0]); // CONCERT/ROCK
}
