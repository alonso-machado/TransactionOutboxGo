// Test 6.1 — two-phase latency baseline (P95/P99), consumers PINNED at 1
// replica/queue (KEDA disabled or paused). See loadtest/README.md.
//
// Phase A (5 min): 100 VUs round-robin across all 5 methods.
// Phase B (5 min): 100 VUs hit PIX only, to contrast a single hot queue
// against the spread-out mixed load and show the single-consumer drain
// ceiling building backlog.
import http from "k6/http";
import { check } from "k6";
import { buildBody, METHODS } from "./payloads.js";

export const options = {
  scenarios: {
    mixed: {
      executor: "constant-vus",
      vus: 100,
      duration: "5m",
      exec: "mixed",
      startTime: "0s",
    },
    pixOnly: {
      executor: "constant-vus",
      vus: 100,
      duration: "5m",
      exec: "pixOnly",
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

function post(method) {
  const body = buildBody(method);
  const res = http.post(`${BASE}/api/v1/payments`, JSON.stringify(body), {
    headers: {
      "Content-Type": "application/json",
      "Idempotency-Key": body.__idempotencyKey,
    },
    tags: { method },
  });
  check(res, { "is 201": (r) => r.status === 201 });
}

export function mixed() {
  post(METHODS[__ITER % METHODS.length]);
}

export function pixOnly() {
  post("PIX");
}
