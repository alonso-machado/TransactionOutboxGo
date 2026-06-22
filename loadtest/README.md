# Load tests (Phase 3 Track 6)

Three k6 scripts, run separately with **opposite consumer setups** — don't
mix them up:

| Script | Drives | Consumer capacity | Measures |
|---|---|---|---|
| `k6-baseline.js` | HTTP `POST /payments` | **pinned at 1 pod/queue** (KEDA disabled/paused) | P95/P99 latency at a fixed, known capacity |
| `k6-autoscale.js` | HTTP `POST /payments` | **KEDA active** (`min 0 / max 10`, target 1000 msgs) | autoscaling: scale-up past 1000 backlog, then scale-down to 0 |
| `k6-consumer.js` | AMQP publish straight onto a queue (bypasses ingestion-api) | pinned 1 pod for a clean number, or unpinned | consumer-worker's own drain rate + consume→persist latency, DB write included |

## 6.1 — Latency baseline

```bash
make loadtest-up
make loadtest TARGET_URL=http://localhost:8080 VUS=20
make loadtest-report TARGET_URL=http://localhost:8080 VUS=20   # + summary.json
```

Two 5-minute phases at `VUS` virtual users: Phase A round-robins all 5
methods, Phase B hits PIX only. **Pin every consumer to 1 replica first** —
both compose files already run a single instance per consumer (leave it);
on Kubernetes, set the KEDA `ScaledObject`'s `minReplicaCount`/
`maxReplicaCount` to 1, or annotate it `autoscaling.keda.sh/paused-replicas: "1"`.

Reports the full k6 default summary (P95/P99, RPS, failure rate, the works) —
every request is tagged `{ method: ... }` so the summary/JSON export can be
sliced per method to find the bottleneck. Watch `dropped_iterations`: if
non-zero, the load generator didn't sustain the target rate.

## 6.2 — Autoscaling (Kubernetes-only)

```bash
make loadtest-autoscale METHOD=PIX TARGET_URL=<ingestion-api-url>
```

Floods one method (default `PIX`) at a rate that outpaces its single
consumer, driving that method's queue backlog past the KEDA trigger
(`queueLengthValue: "1000"`, see `helmcharts/transaction-outbox/values.yaml`).
**Do not pin consumers for this test** — remove any `paused-replicas`
annotation left over from 6.1 first. Watch `kubectl get pods,scaledobject -n
transaction-outbox`: the target method's consumer should scale 0→1→…→10 (and
never above), then back to 0 once the load stage ends and the queue drains
(after `cooldownPeriod`). The scaling is the system under test, observed
out-of-band — k6 only produces the load.

## 6.3 — Consumer-worker in isolation

Needs a custom k6 binary (xk6-amqp + xk6-sql). `make loadtest-up`'s single
PIX consumer is exactly what this test wants by default (`METHOD=PIX`) —
use it instead of the full stack here too.

```bash
make loadtest-up
make k6-ext-build
make loadtest-consumer METHOD=PIX N=100000 \
  RABBITMQ_URL=amqp://guest:guest@localhost:5672/ \
  DATABASE_URL="postgres://outbox:outbox@localhost:5432/outbox?sslmode=disable"
```

Publishes `N` messages straight onto `payments.<method>.queue` (the exact
shape `DispatchOutbox`'s publisher puts on the wire — bypassing
ingestion-api entirely), then polls `payments` until the batch is drained.
Reports `messages_persisted`, `consume_to_persist_latency` (p95/p99), and
the standard k6 summary for the publish side. On a small machine, drop `N`
well below the 100000 default (e.g. `N=5000`) — it's a one-instance consumer
draining a single queue, not a throughput ceiling worth measuring on
4 cores.

- `SCENARIO=drain` (default) — clean throughput number, every message a
  distinct `paymentId`/`source_message_id`.
- `SCENARIO=dedup` — a `DUP_FRACTION` (default `0.1`) of messages reuse a
  prior iteration's identity, simulating RabbitMQ redelivery; asserts the
  final `payments` count matches the distinct-key count, proving the
  consumer's `ON CONFLICT (source_message_id) DO NOTHING` dedup holds under
  concurrency.

**Safety:** point this at a load/test database only — `DATABASE_URL` must
never be the production DSN. Pin the target method's consumer to 1 pod for a
clean per-consumer number; an unpinned rerun lets the same backlog drive KEDA
0→N→0 while rows stream into `payments`, mirroring 6.2 from the queue side.

> The xk6-amqp/xk6-sql API calls in `k6-consumer.js` are a best-effort sketch
> against the extensions' published interfaces — verify them against
> whatever version `k6-ext-build` actually resolves before trusting the
> numbers; extension APIs aren't as stable as k6 core.

## Shared

`payloads.js` builds a valid wire-format body for every method
(`PIX`/`BOLETO`/`TRANSFER`/`CARTAO_CREDITO`/`CARTAO_DEBITO`) with a unique
`eventId`/`Idempotency-Key` per call — without that, dedup collapses every
iteration into one outbox row and the test measures nothing.
