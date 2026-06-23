# Load tests (Phase 3 Track 6)

Three k6 scripts, run separately with **opposite consumer setups** — don't
mix them up:

| Script | Drives | Consumer capacity | Measures |
|---|---|---|---|
| `k6-baseline.js` | HTTP `POST /payments` | **pinned at 1 pod/queue** (KEDA disabled/paused) | P95/P99 latency at a fixed, known capacity |
| `k6-autoscale.js` | HTTP `POST /payments` | **KEDA active** (`min 0 / max 10`, target 1000 msgs) | autoscaling: scale-up past 1000 backlog, then scale-down to 0 |
| `k6-consumer.js` | AMQP publish straight onto a queue (bypasses ingestion-api) | pinned 1 pod for a clean number, or unpinned | queue backlog/drain rate (RabbitMQ side) + consumer-worker's outcome breakdown (`ack`/`duplicate`/`unknown_schema_version`/...) via its `/metrics`, **DB write included** but checked separately, not by k6 |

## 6.1 — Latency baseline

```bash
make loadtest-up
make loadtest TARGET_URL=http://localhost:8080
make loadtest-report TARGET_URL=http://localhost:8080   # + summary.json
```

Two 5-minute phases at a fixed 100 virtual users: Phase A round-robins all 5
methods, Phase B hits PIX only. **Pin every consumer to 1 replica first** —
both compose files already run a single instance per consumer (leave it);
on Kubernetes, set the KEDA `ScaledObject`'s `minReplicaCount`/
`maxReplicaCount` to 1, or annotate it `autoscaling.keda.sh/paused-replicas: "1"`.

Reports the full k6 default summary (P95/P99, RPS, failure rate, the works) —
every request is tagged `{ method: ... }` so the summary/JSON export can be
sliced per method to find the bottleneck. Watch `dropped_iterations`: if
non-zero, the load generator didn't sustain the target rate.

### Results (local compose, 2026-06-23)

`make loadtest-up` subset only (`postgres` + `rabbitmq` + `ingestion-api` +
`consumer-pix` — **not** the other 4 consumers), default config, 6-vCPU/2GB
Podman VM. Numbers below are the baseline to diff future runs against.

| Metric | Value |
|---|---|
| Total requests | 811,065 |
| Throughput | 1,351 req/s |
| P50 | 48.24ms |
| P90 | 141.59ms |
| **P95** | **199.68ms** (threshold `<500ms` ✓) |
| **P99** | **465.22ms** (threshold `<1000ms` ✓) |
| Max | 2.84s |
| Error rate | 0.00% (threshold `<1%` ✓) |
| Outbox `NEW` (undispatched) at test end | 800,165 |
| Outbox `PUBLISHED` at test end | 10,900 |
| `payments` rows persisted | 2,218 (PIX only — only `consumer-pix` was running) |

**Key finding: the HTTP/DB write path is not the bottleneck — `DispatchOutbox`'s
default tuning is.** `OUTBOX_DISPATCH_BATCH_SIZE=50` every
`OUTBOX_DISPATCH_INTERVAL_MS=500` is a hard ceiling of ~100 rows/sec
dispatched to RabbitMQ, regardless of how fast rows are inserted. At
1,351 req/s sustained for 10 minutes (811K inserts), the dispatcher could only
ever clear ~60,000 of them in real time — hence the 800K-row `NEW` backlog.
ingestion-api's own write path (Gin → Postgres `INSERT ... ON CONFLICT`) kept
up fine; `DispatchOutbox`'s poll batch/interval is the actual throughput
ceiling for outbox→RabbitMQ delivery at this load level. Bumping
`OUTBOX_DISPATCH_BATCH_SIZE`/lowering `OUTBOX_DISPATCH_INTERVAL_MS` (or
relying more on the LISTEN/NOTIFY wakeup, Phase 5 Track 3.A, for lower
*latency* on the first row of a batch — it doesn't raise the *per-batch* cap)
is the lever to pull if real-time dispatch at this rate matters.

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

Needs a custom k6 binary with the `xk6-amqp` extension (`build/k6/Dockerfile`).
k6 only does the publish side here — **consumer-worker's own behavior is
read from its Prometheus `/metrics` endpoint**, not from k6 — see "Checking
consumer behavior" below.

```bash
make loadtest-up
make k6-ext-build
make loadtest-consumer METHOD=PIX N=50000 \
  RABBITMQ_URL=amqp://guest:guest@localhost:5672/
```

Publishes `N` messages at a **fixed 100 VUs** straight onto
`payments.<method>.queue` (the exact shape `DispatchOutbox`'s publisher puts
on the wire — bypassing ingestion-api entirely), hitting RabbitMQ as hard as
it can. Every run is a **mix** of three message shapes by default, so a
single invocation exercises all of the consumer's outcomes in one pass:

- the rest — unique, well-formed → `outcome=ack`
- `DUP_FRACTION` (10%) — reuses a prior iteration's identity → `outcome=duplicate`
- `SCHEMA_FRACTION` (2%) — unrecognized `schemaVersion` → `outcome=unknown_schema_version`
  (rejected straight to the DLQ on the first attempt, no retry wait)

Set either fraction to `0` (`-e DUP_FRACTION=0`) for a clean-only run.

### Checking consumer behavior

```bash
# Snapshot before, then again after the run finishes draining, and diff:
curl -s http://localhost:9091/metrics | grep consumer_messages_processed_total
curl -s -u guest:guest http://localhost:15672/api/queues/%2F/payments.pix.queue | grep -o '"messages":[0-9]*'
curl -s -u guest:guest http://localhost:15672/api/queues/%2F/payments.pix.dlq | grep -o '"messages":[0-9]*'
```

`consumer_messages_processed_total{outcome=...}` is the per-outcome counter
(`ack`/`duplicate`/`retry_scheduled`/`poison_dlq`/`unknown_schema_version`);
the delta across the run should always sum to exactly `N` — no message is
ever silently lost (see Results below for a worked example).

### Results (local compose, 2026-06-23)

`make loadtest-up` subset (`postgres` + `rabbitmq` + `ingestion-api` +
`consumer-pix` only), `N=50000`, default fractions, 6-vCPU/2GB Podman VM.

| Metric | Value |
|---|---|
| Published | 50,000 in 17.2s (2,899 msg/s) |
| Flagged duplicate / bad-schema at publish time | 4,967 / 946 |
| Peak queue backlog observed | 42,432 (consumer couldn't keep up with publish burst) |
| Backlog fully drained | within ~1 minute of publish finishing |
| `outcome=ack` (fresh saves) | 48,317 |
| `outcome=duplicate` | 737 (lower than 4,967 flagged — see below) |
| `outcome=unknown_schema_version` | 946 |
| **Reconciliation** | 48,317 + 737 + 946 = **50,000** — exact match, zero message loss |
| DLQ depth after drain | 946 (exactly the bad-schema count) |

**Why only 737 of the 4,967 flagged "duplicate" publishes actually landed as
`outcome=duplicate`:** the script flags a message as a duplicate by reusing
iteration `__ITER-1`'s identity, but with `shared-iterations` spread across
100 concurrent VUs, iteration order isn't sequential — `__ITER-1` may not
have been published yet (or may itself have been schema-flagged) by the time
its "duplicate" pairs with it. This is a property of the publish-side
simulation under concurrency, not a consumer bug — the reconciliation above
(every one of the 50,000 published messages is accounted for in exactly one
outcome bucket) is the thing that actually matters.

Pin the target method's consumer to 1 pod for a clean per-consumer number;
an unpinned rerun lets the same backlog drive KEDA 0→N→0, mirroring 6.2
from the queue side.

## Shared

`payloads.js` builds a valid wire-format body for every method
(`PIX`/`BOLETO`/`TRANSFER`/`CARTAO_CREDITO`/`CARTAO_DEBITO`) with a unique
`eventId`/`Idempotency-Key` per call — without that, dedup collapses every
iteration into one outbox row and the test measures nothing.
