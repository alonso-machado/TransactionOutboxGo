// Test 6.2 — autoscaling: floods ONE method's queue past KEDA's
// queueLengthValue trigger (1000, see helmcharts/transaction-outbox
// values.yaml consumerWorker.keda) to force scale-up, then stops producing
// so the queue drains and KEDA scales the consumer back to 0.
//
// Consumers must NOT be pinned for this test (the opposite of 6.1) — run
// against the real KEDA config: minReplicaCount 0, maxReplicaCount 10. The
// scaling itself is observed out-of-band via `kubectl get pods/scaledobject`
// — k6 only produces the load.
//
// Kubernetes-only (KEDA needs K8s) — not run against local compose.
import http from "k6/http";
import { check } from "k6";
import { buildBody } from "./payloads.js";

const METHOD = __ENV.METHOD || "PIX";
const BASE = __ENV.TARGET_URL || "http://localhost:8080";

export const options = {
  scenarios: {
    flood: {
      executor: "ramping-arrival-rate",
      startRate: 200,
      timeUnit: "1s",
      preAllocatedVUs: 200,
      maxVUs: 1000,
      stages: [
        { target: 2000, duration: "2m" }, // ramp the produce rate up
        { target: 2000, duration: "5m" }, // hold — backlog climbs, KEDA scales toward 10
        { target: 0, duration: "1m" }, // stop producing; queue drains, KEDA scales back to 0
      ],
    },
  },
};

export default function () {
  const body = buildBody(METHOD);
  const res = http.post(`${BASE}/api/v1/payments`, JSON.stringify(body), {
    headers: {
      "Content-Type": "application/json",
      "Idempotency-Key": body.__idempotencyKey,
    },
    tags: { method: METHOD },
  });
  check(res, { "is 201": (r) => r.status === 201 });
}
