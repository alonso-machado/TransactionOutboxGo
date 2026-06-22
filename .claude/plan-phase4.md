# Plan Phase 4 — Rate Limiting · Canary Deployments · Grafana Dashboards · TimescaleDB

> **When to execute:** after Phase 3 is complete and the stack runs end-to-end
> with per-method queues, card payments, CI/CD, the `helmcharts/transaction-outbox`
> chart, Pulumi/AWS and the k6 suite in place.
> **Prerequisite:** `.claude/plan-phase3.md` implemented; `make up`,
> `make test-integration` and the two GitHub Actions workflows all green.

---

## Overview

Four feature tracks plus the usual cross-cutting track. Recommended execution
order — each builds the substrate the next one observes or deploys:

| # | Track | Why this order |
|---|---|---|
| 1 | **Edge protection: ALB + WAF + IP-based leaky-bucket limiter** | Moves the front door NLB→ALB (real client IP + WAF attach point), adds AWS WAF/Shield DDoS protection, and an in-process per-IP leaky bucket; emits a `429` metric Track 3 renders. The ALB also unlocks Track 4's exact request-weighted canary |
| 2 | **TimescaleDB — per-method hypertables, daily chunks** | Data-layer change touching the consumer-worker write path + dedup semantics; its new metrics (chunk count, compression) feed Track 3's infra dashboard |
| 3 | **Grafana + Prometheus + dashboards** | Visualizes Tracks 1–2 and the existing Phase 2/3 metrics; one dashboard per microservice + one infra dashboard (RabbitMQ + Postgres/Timescale) |
| 4 | **Canary deployments (0→5→20→50→100, manual-gated)** | Deployment strategy layered on top; its *optional* automated analysis can consume Track 3's Prometheus, but the headline requirement (manual approval at every step) needs no metrics |
| 5 | **Cross-cutting** | Config, docker-compose, Helm/Pulumi, tests and docs that span the tracks |

**Guiding principle (unchanged):** the Clean Architecture dependency rule still
holds. `internal/domain` and `internal/usecase` must never import Gin, GORM or
`amqp091-go`. The rate-limiter lives in the HTTP adapter; the per-method table
routing lives in the persistence adapter; the ALB/WAF/Shield edge,
Grafana/Prometheus, and Argo Rollouts are pure infrastructure (compose + chart +
Pulumi) and touch no Go domain code.

---

## Track 1 — Edge Protection: ALB + WAF + IP-Based Rate Limiting (ingestion-api)

### Goal

Protect `ingestion-api` (the single public front door — see Phase 3 Track 4's
network boundary) with **defense in depth**, keyed on the **client IP** (we don't
have richer per-client identity to bucket on):

1. **Edge (AWS):** an **ALB** front door + **AWS WAF** rate-based rule + managed
   rule groups, with **Shield Standard** always-on underneath — coarse per-IP
   throttling and L3/L4/L7 DDoS mitigation *before* traffic reaches the cluster.
2. **App (ingestion-api):** a **leaky-bucket** limiter that throttles each
   **client IP** to a steady leak rate `r` req/s with burst `b`, returning
   `429 Too Many Requests` + `Retry-After`. This is the fine-grained last line,
   and the **only** layer that exists locally (compose) where there's no WAF.

> **Front-door change (the enabling decision).** Phase 3 put an **NLB** in front
> of ingestion-api. An NLB is layer-4 (no `X-Forwarded-For`) and, with the
> default `externalTrafficPolicy: Cluster`, kube-proxy **SNATs the source to the
> node IP** — so the pod never sees the real client IP, making IP-based limiting
> meaningless. **AWS WAF also cannot attach to an NLB.** Both problems are solved
> by moving the front door to an **ALB Ingress** (AWS Load Balancer Controller):
> the ALB injects `X-Forwarded-For` (real client IP for the app limiter) **and**
> is a valid WAF association target. The security boundary is unchanged — the ALB
> sits in the public subnets and targets the private-subnet pods exactly as the
> NLB did; ingestion-api remains the *only* publicly reachable component.

### What "leaky bucket" means here (algorithm)

The classic **leaky-bucket-as-a-meter** (a GCRA-equivalent, the same shape as a
token bucket but framed as a draining queue):

- Each key has a `level` (current water) and a `lastLeak` timestamp.
- On each request: `leaked = elapsed_seconds * r`; `level = max(0, level - leaked)`;
  then if `level + 1 > b` → **reject (429)**; else `level += 1` and accept.
- Steady state: the bucket drains at a constant `r` req/s no matter how bursty
  the arrival pattern — the defining property of a leaky bucket (a token bucket,
  by contrast, refills tokens; the two are mathematically dual, and the
  "drain at constant rate" framing is the one the requirement names).

`Retry-After` is computed from how long until enough has leaked to admit one
request: `ceil((level + 1 - b) / r)` seconds.

### 1.A — Front door: NLB → ALB Ingress (the real-client-IP enabler)

- **Chart:** ingestion-api's `Service` becomes a plain `ClusterIP` again (the ALB
  targets it); add an **`Ingress`** for ingestion-api gated by an
  `ingestionApi.ingress.enabled` flag (default false locally, true on EKS).
  Annotations:
  ```
  kubernetes.io/ingress.class: alb
  alb.ingress.kubernetes.io/scheme: internet-facing
  alb.ingress.kubernetes.io/target-type: ip          # target pods directly (private subnets)
  alb.ingress.kubernetes.io/listen-ports: '[{"HTTP":80}]'
  alb.ingress.kubernetes.io/wafv2-acl-arn: <web-acl-arn>   # set by Pulumi (1.B)
  ```
- **Pulumi:** install the **AWS Load Balancer Controller** (Helm release, same
  pattern as `keda.go`); `workloads.go` drops the NLB `Service.type:
  LoadBalancer` override and instead sets `ingestionApi.ingress.enabled=true` +
  the WAF ACL ARN. The controller provisions a public-subnet ALB targeting the
  private-subnet pods — identical exposure topology to the Phase 3 NLB.
- **Client-IP extraction in Go (the part to get right):** the ALB appends the
  genuine client IP as the **last** `X-Forwarded-For` entry. Configure Gin
  `router.SetTrustedProxies(<VPC/private-subnet CIDRs>)` and read `c.ClientIP()`
  — it returns the rightmost non-trusted IP, i.e. the ALB-appended real client,
  ignoring any client-spoofed earlier XFF entries.
  - **Security:** Gin's *default* trusts all proxies (`0.0.0.0/0`), which lets a
    client spoof XFF. **Must** set `SetTrustedProxies` explicitly to the VPC
    CIDRs (sourced from `network.go`). Locally (no ALB, no XFF) `c.ClientIP()`
    falls back to the direct `RemoteAddr`, which is correct for compose.

### 1.B — AWS WAF + Shield (DDoS protection)

- **Shield Standard** — free, automatic, always-on for the ALB (L3/L4
  SYN-flood/reflection). No config; documented as active.
- **AWS WAF (`wafv2`) Web ACL**, associated with the ALB via the
  `alb.ingress.kubernetes.io/wafv2-acl-arn` annotation. New Pulumi file
  `infra/pulumi/waf.go` creates a `wafv2.WebAcl` (scope `REGIONAL`) with:
  - **Rate-based rule** — `RateBasedStatement` blocking an IP that exceeds
    *N requests / 5-min sliding window* (e.g. 2000). This is **edge per-IP rate
    limiting**, complementary to the app leaky bucket: it sheds volumetric floods
    before they ever reach a pod.
  - **Managed rule groups** — `AWSManagedRulesCommonRuleSet`,
    `AWSManagedRulesKnownBadInputsRuleSet`, `AWSManagedRulesAmazonIpReputationList`
    (drops known-malicious/bot IPs).
  - WAF logging → CloudWatch (or the Track 3 stack) so blocks are observable.
- **Shield Advanced** — deliberately **out of scope** (~US$3k/mo); noted as the
  enterprise upgrade for L7 auto-mitigation + DDoS cost protection.
- **Two rate limiters, on purpose:** WAF is the coarse, distributed,
  edge-enforced per-IP cap (protects the *cluster*); the app leaky bucket (1.C)
  is the fine-grained, per-IP, in-process cap (protects the *service logic*, and
  is the only one present in local dev). They overlap intentionally — that's
  defense in depth, not redundancy.

### 1.C — App-level IP leaky bucket

Key = the client IP resolved in 1.A. No request-body inspection is needed (a big
simplification over a payload-derived key — the middleware never touches the
body), so the limiter runs cheaply before the handler.

### Decision: where the bucket state lives (in-memory vs Redis)

**Recommendation — in-memory per-instance** for this phase, with Redis documented
as the production-correctness option:

- In-memory: a `map[string]*bucket` guarded by a `sync.Mutex` (or sharded
  mutexes), with a background janitor goroutine evicting keys idle longer than a
  TTL so the map can't grow unbounded.
- **Caveat to document, not hide:** `ingestion-api` runs behind an HPA
  (`minReplicas: 1, maxReplicas: 10`). With N replicas and no shared store, the
  *effective* global limit per key is `N × r` — each pod meters independently.
  For a faithful global limit, the bucket store must be shared (Redis /
  ElastiCache, `INCR`+TTL or a Lua GCRA script). That's a larger lift (new
  dependency, new Pulumi resource) and is **out of scope here** — captured as a
  hardening item. The limiter is written against a small `BucketStore` interface
  so swapping the in-memory impl for a Redis one later is a one-file change.
  - **Why per-pod is acceptable here:** the WAF rate-based rule (1.B) is the
    *cluster-wide* per-IP cap enforced at the edge across all pods; the app
    limiter is the per-pod fine-grained backstop. The cross-pod inaccuracy of
    the in-memory store is exactly what the WAF layer covers, so a shared store
    is a refinement, not a correctness gap.

### Clean Architecture placement

- The limiter is a **Gin middleware** in the HTTP adapter — it's an HTTP-boundary
  concern, same layer as `otelgin`/`Recovery`/`Logger`.
- The leaky-bucket algorithm itself is pure (no framework) and goes in a small
  `ratelimit` package with a `BucketStore` interface + an in-memory impl — keeps
  it unit-testable without Gin and ready for a Redis impl. It does **not** belong
  in `internal/domain` (it's not a payments-domain concept; it's transport
  protection), so it lives under the adapter, e.g.
  `internal/adapter/http/ratelimit/`.

### Files to add / modify

| File | Change |
|---|---|
| `internal/adapter/http/ratelimit/bucket.go` *(new)* | Pure leaky-bucket meter + `BucketStore` interface + in-memory store with TTL eviction janitor; unit-tested |
| `internal/adapter/http/ratelimit/middleware.go` *(new)* | Gin middleware: key on `c.ClientIP()`, call the meter, set `X-RateLimit-Limit`/`X-RateLimit-Remaining`/`Retry-After`, `429` on reject; increments the new metric. No body access |
| `internal/adapter/http/router.go` | `router.SetTrustedProxies(<VPC CIDRs>)` so `c.ClientIP()` reads the ALB-appended XFF; `r.Use(ratelimit.Middleware(...))` **after** `otelgin` (so a 429 still gets a span), applied only to `/api/v1/payments` — exclude `/healthz`, `/metrics`, `/swagger` |
| `internal/infrastructure/config/config.go` | Add `RateLimitEnabled bool`, `RateLimitRate float64` (req/s leak), `RateLimitBurst int` (capacity), `TrustedProxies []string` (CIDRs Gin trusts for XFF) |
| `cmd/ingestion-api/main.go` | Build the store + middleware from config; start the janitor; set trusted proxies; wire into the router |
| `internal/infrastructure/telemetry` (or where metrics are registered) | Register `ingestion.ratelimit_rejected_total` (counter) and optionally `ingestion.ratelimit_allowed_total` |
| `infra/pulumi/waf.go` *(new)* | `wafv2.WebAcl` (rate-based rule + managed rule groups) + logging; exports the ACL ARN for the Ingress annotation |
| `infra/pulumi/albcontroller.go` *(new)* | AWS Load Balancer Controller Helm release (provisions the ALB from the Ingress) |
| `infra/pulumi/workloads.go` | Drop the NLB `Service.type: LoadBalancer` override; set `ingestionApi.ingress.enabled=true` + `wafv2-acl-arn` |
| `helmcharts/transaction-outbox/templates/ingestion-api/ingress.yaml` *(new)* | ALB Ingress (gated by `ingestionApi.ingress.enabled`); `service.yaml` reverts to `ClusterIP` |

### Config (new env vars, ingestion-api only)

```
RATE_LIMIT_ENABLED   default "true"
RATE_LIMIT_RATE      default "50"      # leak rate, requests/second per IP
RATE_LIMIT_BURST     default "100"     # bucket capacity
TRUSTED_PROXIES      default ""        # comma-separated CIDRs Gin trusts for XFF
                                       # (empty = trust none → use direct RemoteAddr, correct locally)
```

### Verification

1. Unit: hammer the meter with a tight loop → admits `b` immediately, then ~`r`
   per second; `Retry-After` math correct; idle eviction frees keys.
2. `make up`; POST `/api/v1/payments` faster than `r` from one client → after the
   burst, requests get `429` with `Retry-After`; from a *different* source IP,
   unaffected (per-IP isolation).
3. `/healthz` and `/metrics` are never rate-limited.
4. `ingestion.ratelimit_rejected_total` increments and is scrapeable at
   `/metrics` (renders on the Track 3 ingestion-api dashboard).
5. **Client-IP correctness on EKS:** with the ALB front door, the limiter buckets
   by the *real* client IP (XFF), not the node IP — verify by hitting it from two
   distinct external IPs and confirming independent buckets. Send a spoofed
   `X-Forwarded-For` → it's ignored (trusted-proxy config), real IP still used.
6. **WAF:** exceed the WAF rate-based threshold from one IP → blocked at the ALB
   with a 403 *before* reaching the pod; a managed-rule match (e.g. a known-bad
   input signature) is likewise blocked at the edge.

---

## Track 2 — TimescaleDB: Per-Method Hypertables, Daily Chunks

### Goal

Move the `payments` write path onto **TimescaleDB** so high insert volume scales
better and old data ages out cheaply. Per the requirement: **partition by day**
and have **one table per payment method**. Concretely: drop the single
`payments` table and create **one TimescaleDB hypertable per method**
(`payments_pix`, `payments_boleto`, `payments_transfer`,
`payments_cartao_credito`, `payments_cartao_debito`), **each chunked at a 1-day
interval**.

### THE critical decision: the time column and the dedup key

This is the subtle, load-bearing part — get it wrong and idempotency breaks.

- **TimescaleDB requires every `UNIQUE`/`PRIMARY KEY` index on a hypertable to
  include all partitioning columns.** So the consumer's current dedup constraint,
  a plain `UNIQUE(source_message_id)`, is **illegal on a hypertable** — it must
  become `UNIQUE(source_message_id, <time_column>)`.
- **If we partition by `created_at` (insert time), dedup BREAKS.** A RabbitMQ
  redelivery of the same message arrives later, gets a *new* `created_at`, so
  `(source_message_id, created_at)` is a *different* tuple → the
  `ON CONFLICT DO NOTHING` no longer fires → a **duplicate payment row**. That
  destroys the consumer's whole idempotency guarantee.
- **Decision: partition by `occurred_at` (the provider's event time), and dedup
  on `(source_message_id, occurred_at)`.** `occurred_at` comes from the wire
  payload and is **deterministic across redeliveries** — a redelivery carries the
  same `occurredAt`, so `(source_message_id, occurred_at)` is stable and
  `ON CONFLICT DO NOTHING` still dedups correctly. Bonus: partitioning payments
  by *business event day* is more meaningful than by *insert day* for reporting.
- This keeps CLAUDE.md's "**no separate inbox table**" rule intact — dedup stays a
  constraint on the payments table(s), just a two-column one now.

> Edge case to note: `occurred_at` far in the past (backfills/replays) creates
> sparse historical chunks. Acceptable; Timescale handles out-of-order inserts.
> If a method legitimately lacks `occurredAt`, the handler already requires it in
> the wire format — keep it required.

### Why per-method tables (Design C) over the alternatives

| Option | Verdict |
|---|---|
| **C. Separate hypertable per method, daily chunks, app routes the insert** | **CHOSEN** — literally "tables by payment_method" + "partition by day"; each table independently chunked, compressed, retained; per-table `UNIQUE(source_message_id, occurred_at)` suffices because a message has exactly one method |
| A. Single multi-dimensional hypertable (time + hash space on `method`) | Rejected — hash buckets aren't human-named per-method tables; `number_partitions` is fixed and won't map 1:1 to the 5 methods cleanly |
| B. Native LIST partition by method, each child a hypertable | Rejected — TimescaleDB does **not** support a hypertable being a partition of a native partitioned parent; the nesting doesn't compose |

A read-side convenience: create a `payments` **`VIEW` = `UNION ALL`** of the five
per-method tables, so ad-hoc "all payments" queries and the Grafana SQL panels
keep working against one name. Writes always target the concrete per-method
table.

### Schema / migration work

GORM `AutoMigrate` **cannot** create extensions, hypertables, compression or
retention policies — so the payments DDL moves to **explicit, idempotent raw
SQL** run at startup (the existing `AutoMigrate` stays for `outbox_messages`).

Per method, the migration runs (idempotent — safe to re-run on every boot):

```sql
CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS payments_pix (
  id                  uuid        NOT NULL,
  source_message_id   text        NOT NULL,
  event_id            text,
  provider_name       text,
  provider_payment_id text,
  external_payment_id text,
  payer_id            uuid,
  recipient_id        uuid,
  amount              bigint,
  currency            text,
  method              text        NOT NULL,
  method_details      jsonb,
  occurred_at         timestamptz NOT NULL,
  created_at          timestamptz NOT NULL DEFAULT now(),
  updated_at          timestamptz NOT NULL DEFAULT now(),
  UNIQUE (source_message_id, occurred_at)        -- dedup; includes the partition col
);
SELECT create_hypertable('payments_pix', 'occurred_at',
                         chunk_time_interval => INTERVAL '1 day',
                         if_not_exists => TRUE);
-- optional, per the showcase:
-- ALTER TABLE payments_pix SET (timescaledb.compress, timescaledb.compress_segmentby = 'method');
-- SELECT add_compression_policy('payments_pix', INTERVAL '7 days', if_not_exists => TRUE);
-- SELECT add_retention_policy('payments_pix', INTERVAL '180 days', if_not_exists => TRUE);
```

…looped over `rmq.Methods` (reuse that canonical list so adding a 6th method
still touches one place). Then the `payments` UNION-ALL view over all five.

**Migration ownership:** `ingestion-api` already runs `AutoMigrate(outbox +
payments)` at startup; it becomes the owner of this raw-SQL migration too (it has
the DB pool first). Generate the table name from the method
(`payments_<method_lowercased>`), matching the queue-name convention.

### Indexes to keep

Beyond the `UNIQUE(source_message_id, occurred_at)` dedup constraint, recreate the
useful secondary indexes per table: `event_id`, `method` (cheap/constant per
table but kept for the view), and rely on the chunk exclusion for time-range
scans (no explicit `occurred_at` index needed — that's the partition key).

### Persistence-adapter change (the only Go change)

- `internal/adapter/persistence/payment_repo.go` — the insert must target the
  per-method table. GORM can't pick a table per row via a static `TableName()`,
  so the repo resolves it: `db.Table(tableFor(p.Method)).Clauses(clause.OnConflict{Columns: {source_message_id, occurred_at}, DoNothing: true}).Create(&model)`.
  `tableFor(method) = "payments_" + strings.ToLower(method)`.
- `internal/adapter/persistence/models.go` — `PaymentModel` loses its single
  `TableName()` (or keeps it pointing at the view for reads); the `OnConflict`
  columns change from `source_message_id` to `(source_message_id, occurred_at)`.
- `internal/adapter/persistence/migrate.go` — keep AutoMigrate for
  `OutboxMessageModel` only; add `MigrateTimescale(db)` running the raw SQL above.
- **No `domain` or `usecase` change** — `domain.Payment` already carries `Method`
  and `OccurredAt`; routing-by-method is a persistence detail behind the
  `PaymentRepository` port. Dependency rule intact.

> **Optional bonus (document, don't necessarily build):** make `outbox_messages`
> a hypertable on `created_at` too, and replace the manual
> `OUTBOX_PRUNE_AFTER_HOURS` prune loop with a Timescale `add_retention_policy`.
> Tidy, but it changes the prune mechanism — flag it, leave the decision to
> implementation time.

### Docker / image change

- `docker-compose.yml`: swap `image: postgres:17` →
  `image: timescale/timescaledb:latest-pg17` (Timescale's Postgres-17 image ships
  the extension preloaded via `shared_preload_libraries`). No other compose change
  — same env vars, same healthcheck.
- Cloud (Pulumi `data.go`): RDS Postgres **does not** offer the full TimescaleDB
  extension. **Decision needed at implementation time** — recommended:
  **Amazon RDS** supports the **`timescaledb` (Apache-licensed/community) extension
  as of recent PG versions** via `shared_preload_libraries` in a parameter group;
  if the target RDS engine version doesn't, fall back to **self-managed Timescale
  on EKS** (StatefulSet) or **Timescale Cloud**. Capture this as the one cloud
  open-question; local dev (compose) is unblocked immediately by the Timescale
  image.

### Tests to adjust

- Integration suite (`tests/integration/`): `TestMain` must run
  `MigrateTimescale` against the TestContainers Postgres — switch the Postgres
  test container image to `timescale/timescaledb:latest-pg17` so the extension
  exists. Assert a PIX insert lands in `payments_pix`, a redelivery (same
  `source_message_id` + `occurred_at`) does **not** create a second row, and the
  `payments` view returns it.
- Add a test proving the **dedup-across-redelivery** guarantee specifically holds
  under the new two-column key (the regression this whole track is most at risk
  of breaking).

### Verification

1. `make up` (Timescale image) → ingestion-api boots, migration creates 5
   hypertables + the view; `\d+ payments_pix` shows it's a hypertable, 1-day
   chunks.
2. POST a PIX → row appears in `payments_pix`; `SELECT * FROM payments` (view)
   shows it; `payments_boleto` empty.
3. Publish the same PIX message twice (redelivery) → exactly **one** row in
   `payments_pix` (dedup on `(source_message_id, occurred_at)` held).
4. `SELECT show_chunks('payments_pix')` after inserting rows with `occurred_at`
   on different days → one chunk per day.
5. (If compression enabled) chunks older than the policy compress; the infra
   dashboard (Track 3) shows compression ratio.

---

## Track 3 — Grafana + Prometheus + Dashboards

### Goal

Stand up a metrics backend (Prometheus scraping the OTel-exposed `/metrics`) and
**Grafana** with provisioned dashboards: **one per microservice** (ingestion-api,
consumer-worker) and **one for infra** (RabbitMQ + Postgres/TimescaleDB).

### Why this shape

Each service already exposes a Prometheus endpoint (Phase 2 — the OTel Prometheus
exporter on `METRICS_PORT/metrics`). RabbitMQ has a **built-in Prometheus plugin**
(`rabbitmq_prometheus`, port `15692`) — no separate exporter needed. Postgres
needs a **`postgres_exporter`** sidecar/service (and a few Timescale-specific
custom queries). So the only new moving parts are **Prometheus**, **Grafana**, and
**postgres_exporter**; everything else is already emitting.

### Local (docker-compose) wiring

Add three services + provisioning:

| Service | Image | Notes |
|---|---|---|
| `prometheus` | `prom/prometheus` | scrape config (below); mounts `observability/prometheus/prometheus.yml` |
| `grafana` | `grafana/grafana` | provisioned datasource + dashboards; admin creds via env; port `3000` |
| `postgres_exporter` | `quay.io/prometheuscommunity/postgres-exporter` | `DATA_SOURCE_NAME` → the Timescale Postgres; custom queries file for Timescale chunks/compression |
| (rabbitmq) | *existing, enable plugin* | enable `rabbitmq_prometheus` + expose `15692` |

Prometheus scrape targets:
- `ingestion-api:9090`
- each `consumer-<method>:909x` (5 targets — the per-method metrics ports already
  mapped in compose)
- `rabbitmq:15692`
- `postgres_exporter:9187`

Grafana provisioning (committed, version-controlled):

```
observability/
├── prometheus/
│   └── prometheus.yml                 # scrape config above
├── postgres-exporter/
│   └── queries.yaml                   # Timescale chunk count, compression ratio, hypertable sizes
└── grafana/
    ├── provisioning/
    │   ├── datasources/prometheus.yml # Prometheus datasource
    │   └── dashboards/dashboards.yml  # file provider → loads the JSON below
    └── dashboards/
        ├── ingestion-api.json
        ├── consumer-worker.json
        └── infra.json
```

### Dashboard contents (what each one shows)

**1. ingestion-api dashboard**
- Request rate (RPS), and **P50/P95/P99 latency** from the otelgin
  `http.server.duration` histogram.
- HTTP status breakdown — 2xx / 4xx / **5xx** rates.
- **Rate-limiter rejections** — `ingestion.ratelimit_rejected_total` rate, broken
  down by `key_kind` (Track 1). A 429 spike panel is the headline new tile.
- **Outbox health** — `outbox.published_total{method=...}` rate (per-method),
  `outbox.pending_count` gauge (backlog of `NEW`/`RETRYING` rows), dispatch
  errors / dead-letter count.
- Process: goroutines, heap, GC pause (Go runtime metrics).

**2. consumer-worker dashboard** (per-method, templated with a `$method` Grafana
variable)
- **Throughput** — `consumer.messages_processed_total` rate by
  `payment_method` / `outcome` (`ack` / `retry_scheduled` / `poison_dlq`).
- **Retries** — `consumer.retry_attempts_total` rate + retry-depth heatmap.
- **Poison/DLQ rate** — `outcome="poison_dlq"` slice.
- **Pod count per method** (KEDA scaling) — kube-state-metrics deployment
  replicas, or in compose just the static 1/method.
- Consume→persist latency (if exported) and per-method backlog cross-referenced
  with the RabbitMQ queue-depth panel.

**3. infra dashboard** (RabbitMQ + Postgres/Timescale)
- **RabbitMQ** (from `rabbitmq_prometheus`): per-queue depth
  (`rabbitmq_queue_messages` for each `payments.<method>.queue` **and** each
  `.dlq`), publish/deliver/ack rates, unacked count, consumer count per queue,
  connection/channel counts, memory/disk alarms.
- **Postgres** (from `postgres_exporter`): connections vs `max_connections`, TPS
  (commits+rollbacks), cache hit ratio, deadlocks, longest transaction, DB size.
- **TimescaleDB** (custom queries): **chunks per hypertable**, **compression
  ratio**, hypertable sizes per method, oldest/newest chunk (retention sanity).

> DLQ-depth and per-queue isolation panels make Phase 3's per-method queue split
> visible at a glance — a CARD poison spike shows only on `payments.cartao_credito.dlq`,
> not the PIX queue.

### Cloud (Pulumi) wiring

Add a Pulumi component (`infra/pulumi/observability.go`) that installs the
**`kube-prometheus-stack`** Helm release (bundles Prometheus + Grafana +
node-exporter + kube-state-metrics) into the cluster, plus a `postgres_exporter`
Deployment pointed at RDS and the RabbitMQ Prometheus endpoint (Amazon MQ exposes
metrics via CloudWatch; for the showcase, scrape the broker's Prometheus plugin
if self-managed, else add a CloudWatch datasource — note this as an
implementation choice). The three dashboard JSONs are loaded via a
`grafana_dashboard`-labeled ConfigMap (the kube-prometheus-stack sidecar
auto-imports labeled ConfigMaps) so the **same committed JSON** serves local and
cloud — single source of truth.

Grafana is **internal-only** in cloud (ClusterIP + port-forward, or an
internal-LB Ingress) — it is *not* a public front door (only ingestion-api is,
per Phase 3 Track 4). Document `kubectl port-forward svc/grafana 3000` for access.

### Files to add / modify

| File | Change |
|---|---|
| `observability/prometheus/prometheus.yml` *(new)* | Scrape config (all app + infra targets) |
| `observability/postgres-exporter/queries.yaml` *(new)* | Timescale chunk/compression custom queries |
| `observability/grafana/provisioning/**` *(new)* | Datasource + dashboard file provider |
| `observability/grafana/dashboards/*.json` *(new)* | The three dashboards above |
| `docker-compose.yml` | Add `prometheus`, `grafana`, `postgres_exporter`; enable RabbitMQ Prometheus plugin + expose `15692` |
| `infra/pulumi/observability.go` *(new)* | kube-prometheus-stack Helm release + postgres_exporter + dashboard ConfigMaps |
| `infra/pulumi/main.go` | Wire the new observability component into the composition |
| `Makefile` | `make observability-up` (compose subset) / doc the Grafana URL + default creds |

### Verification

1. `make up` → `http://localhost:3000` (Grafana) shows three dashboards, all
   datasource-green.
2. `make seed-pix` a few times → ingestion-api dashboard RPS/latency move; outbox
   `published_total{method="PIX"}` ticks; consumer dashboard PIX throughput ticks.
3. Trip the rate limiter (Track 1) → the **429 panel** climbs.
4. Infra dashboard shows `payments.pix.queue` depth, the 5 DLQs at 0, Postgres
   connections, and **Timescale chunk count rising per day** (Track 2).
5. Cloud: `kubectl port-forward svc/...grafana 3000` → same three dashboards from
   the same committed JSON.

---

## Track 4 — Canary Deployments (manual at the start, then time-gated)

### Goal

Every deploy of a service rolls out **progressively**, starting at **0%** (new
version present but taking no traffic). The two services diverge from there —
this is a deliberate revision from a uniform manual-every-step design (see below):

- **ingestion-api:** 0% → human promotes → 5% → human promotes → from there the
  remaining steps (20% → 50% → 100%) advance **automatically every 20 minutes**,
  *unless* a background error-rate analysis (Track 3's Prometheus) regresses, in
  which case the rollout **auto-aborts**. A human can also manually advance faster
  or abort at any point.
- **consumer-worker:** 0% → human promotes → 5% → after **1 hour** at 5% with no
  manual action, it advances **directly to 100%** (no 20/50 intermediate steps —
  a queue consumer's "blast radius" concern is adequately bounded by the 1-hour
  soak at 5%, so the extra steps just add latency to a full rollout without much
  extra safety here).
- **Testing the canary at 0% (or any weight) deliberately:** requests carrying the
  header `canary: true` are *always* routed to the canary pods regardless of the
  current weight — see "Header-based forced routing" below. This is what makes a
  0%-weight canary useful: you can hit the literal new version on demand before
  any real traffic does, without waiting for a promote.

### Decision: Argo Rollouts (not Flagger, not raw Deployments)

- **Argo Rollouts** models the gated steps with a `Rollout` resource whose
  `strategy.canary.steps` mixes **`pause: {}`** (an *indefinite* pause — holds
  until an operator runs `kubectl argo rollouts promote`) for the manual part,
  and **`pause: {duration: 20m}`** / **`pause: {duration: 1h}`** (a *timed* pause
  — auto-resumes) for the automatic part. A `strategy.canary.analysis` background
  AnalysisTemplate runs continuously once real traffic starts and can auto-abort
  the whole rollout — that's the "if errors did not go up you can go further"
  half of the requirement; the timed pauses are the "20 minutes each part" half.
  **Chosen.**
- **Flagger** is rejected: it's built around *fully automated*, metric-driven
  promotion from step zero — it doesn't have a clean way to express "manual for
  the first two steps, then automatic," which is exactly what's needed here.
- Raw `Deployment` `RollingUpdate` can't express weighted canary steps, pause
  gates, or header-based routing at all.

### Step definitions (now per-service, not shared)

**ingestion-api:**

```yaml
strategy:
  canary:
    steps:
      - setWeight: 0
      - pause: {}                 # MANUAL — lands at 0%, test via the canary:true header
      - setWeight: 5
      - pause: {}                 # MANUAL — approve to leave 5% and go automatic
      - setWeight: 20
      - pause: { duration: 20m }  # AUTO — promotes after 20m unless analysis fails
      - setWeight: 50
      - pause: { duration: 20m }  # AUTO
      - setWeight: 100            # AUTO — full promotion
    analysis:
      templates:
        - templateName: ingestion-api-error-rate
      startingStep: 2             # background analysis starts once weight leaves 0%
```

**consumer-worker** (per method, pod-proportion semantics — see below):

```yaml
strategy:
  canary:
    steps:
      - setWeight: 0
      - pause: {}                 # MANUAL — lands at 0%
      - setWeight: 5
      - pause: { duration: 1h }   # AUTO — after 1h at 5%, skip straight to 100%
      - setWeight: 100
```

The leading `setWeight: 0` + indefinite `pause: {}` is what makes **every deploy
start at 0% and block** — the new ReplicaSet exists but (for ingestion-api) takes
no weighted traffic and (for consumer-worker) processes no messages until the
first manual promote.

### Header-based forced routing to the canary (ingestion-api only)

Argo Rollouts' ALB integration manages exactly **one** weighted forwarding rule
on the shared ALB (the rule it mutates on every `setWeight`) — it does not support
header-match routing the way the Istio/SMI/Nginx/Traefik providers do. The
standard workaround, and the one used here: put a **second, independent**
`Ingress` object on the **same ALB** (same
`alb.ingress.kubernetes.io/group.name`, lower `group.order` so it's evaluated
*before* the Rollout-managed weighted rule) with a header-match condition that
forwards unconditionally to the canary `Service`:

```yaml
metadata:
  annotations:
    alb.ingress.kubernetes.io/group.name: ingestion-api
    alb.ingress.kubernetes.io/group.order: "1"     # evaluated before the weighted rule (order 10)
    alb.ingress.kubernetes.io/conditions.canary-header: |
      [{"field":"http-header","httpHeaderConfig":{"httpHeaderName":"canary","values":["true"]}}]
spec:
  rules:
    - http:
        paths:
          - path: /api/v1/payments
            pathType: Prefix
            backend:
              service:
                name: ingestion-api-canary
                port: { number: 8080 }
```

This Ingress is **not** touched by Argo Rollouts (it only owns the
weighted-rule Ingress) — it's a static, always-100%-to-canary rule that exists
independently of the current canary weight, which is exactly what's needed to
exercise the canary at 0% before promoting anything. Requires
`stableService`/`canaryService` to be named explicitly in
`trafficRouting.alb` so there's a concrete canary `Service` to target.

### Traffic splitting: how the % is realized

**Now that Track 1 moves ingestion-api behind an ALB and installs the AWS Load
Balancer Controller, the canary gets *exact* request-weighted splitting for
free** — Argo Rollouts has native `trafficRouting.alb` support that manipulates
the ALB target-group weights, so "5%" means 5% of *requests*, not 5% of pods.

| Approach | Applies to | Verdict |
|---|---|---|
| **ALB `trafficRouting`** | **ingestion-api** (it has the ALB Ingress from Track 1) | **CHOSEN** — precise request-level 5/20/50/100; the controller is already installed for Track 1, so no new dependency. Rollout points `trafficRouting.alb.ingress` at the Track 1 Ingress |
| **Replica-weight canary (no traffic router)** | **consumer-worker** (no Service/Ingress — nothing to weight) | **CHOSEN** for the consumer side — the weight controls the canary/stable **pod proportion**, which is the only meaningful interpretation for a queue consumer (see below) |

### consumer-worker is a special case (no Service, no inbound traffic)

`consumer-worker` has **no Kubernetes Service** (Phase 3 Track 4 — nothing calls
it; it pulls from its queue). "5% of traffic" is meaningless for a queue consumer.
For it, the canary still has real value as a **progressive version rollout**: the
weight controls the **fraction of consumer pods** running the new image, so a bad
new version only processes a fraction of messages before the operator promotes or
aborts. Same `steps` block, interpreted as pod-proportion only (which is exactly
what the no-trafficRouting mode does anyway). Document this difference explicitly:
*ingestion-api canary = request share; consumer-worker canary = pod share / blast
radius of the new version.*

> KEDA interaction: a KEDA `ScaledObject` can target an Argo `Rollout` (set
> `scaleTargetRef.apiVersion: argoproj.io/v1alpha1, kind: Rollout`). The per-method
> ScaledObjects from Phase 3 must be **re-pointed from `Deployment` to `Rollout`**
> when the consumer-worker template is converted — this is the one non-obvious
> wiring detail and a likely source of a "scaling stopped working" bug if missed.

### Chart changes

Convert the workload templates from `Deployment` to `Rollout`:

| File | Change |
|---|---|
| `helmcharts/transaction-outbox/templates/ingestion-api/deployment.yaml` | → `rollout.yaml`: `kind: Rollout`, add the `strategy.canary.steps` above + `trafficRouting.alb` pointing at the Track 1 ALB Ingress (exact request-weighted splitting) |
| `helmcharts/transaction-outbox/templates/consumer-worker/deployment.yaml` | → `rollout.yaml`: same steps (pod-proportion semantics); one per method as today |
| `helmcharts/transaction-outbox/templates/consumer-worker/scaledobject.yaml` | `scaleTargetRef` → `kind: Rollout, apiVersion: argoproj.io/v1alpha1` |
| `helmcharts/transaction-outbox/templates/ingestion-api/hpa.yaml` | Either drop (Rollout manages stable/canary replica counts) or convert to a Rollout-aware HPA targeting the Rollout; **decision at impl time** — simplest is to let the Rollout own ingestion-api replica counts and remove the HPA |
| `helmcharts/transaction-outbox/values.yaml` | Add `canary.steps` (parameterized so the weights/pauses are configurable) and `canary.enabled` (fall back to a plain rolling Deployment when false, e.g. for `make up` local where Argo Rollouts isn't installed) |
| `helmcharts/transaction-outbox/Chart.yaml` | Bump version |

> **`canary.enabled` matters for local dev:** Argo Rollouts' CRDs/controller don't
> exist in plain docker-compose, and likely not in a bare `kind`/`minikube`
> either. Gate the `Rollout` behind `canary.enabled` (default false in
> `values.yaml`, true in the Pulumi `--set` for EKS) so the chart still renders a
> normal `Deployment` everywhere Argo Rollouts isn't installed.

### Pulumi changes

- `infra/pulumi/argorollouts.go` *(new)* — install the **Argo Rollouts
  controller** via Helm release (upstream `argo-rollouts` chart), same pattern as
  `keda.go`.
- `infra/pulumi/workloads.go` — `--set canary.enabled=true` on the app chart
  release; ensure the Argo Rollouts release is a dependency (controller before
  workloads).
- `infra/pulumi/main.go` — wire the new component.

### Manual approval — how the human promotes

Three options, document all, recommend the first two together:

1. **Argo Rollouts dashboard / `kubectl-argo-rollouts` plugin** —
   `kubectl argo rollouts get rollout ingestion-api --watch` shows the paused
   step; `kubectl argo rollouts promote ingestion-api` advances one step (or
   `--full` to skip to 100%); `… abort` rolls back to stable. This is the
   ground-truth control.
2. **GitHub Actions promote workflow gated by Environments** *(ties into Phase 3
   CI/CD)* — a `workflow_dispatch` "promote" job per service, with **one GitHub
   `environment` per percentage** (`canary-5`, `canary-20`, `canary-50`,
   `canary-100`), each carrying a **required-reviewers protection rule**. Clicking
   "approve" in the GitHub Environments UI is the manual gate; the job then runs
   `kubectl argo rollouts promote`. This makes the approval auditable in GitHub
   alongside the Phase 3 `deploy` job. The Phase 3 `deploy` job (`pulumi up`) just
   ships the new image → the Rollout auto-pauses at 0% → this promote workflow
   drives it up.
3. **Background analysis gate (ingestion-api, not optional anymore)** — an Argo
   `AnalysisTemplate` querying Track 3's **Prometheus** for the canary's 5xx rate
   runs continuously from `startingStep` onward and auto-aborts the rollout if it
   regresses versus stable — this is what backs the "if errors did not go up you
   can go further" half of the per-service step definitions above. It runs
   alongside, not instead of, the manual/timed `pause` steps.

### Verification

1. `make infra-up` (EKS) → Argo Rollouts controller running;
   `kubectl get rollouts -n transaction-outbox` lists ingestion-api + 5
   consumer rollouts.
2. CI deploys a new image SHA → `kubectl argo rollouts get rollout ingestion-api`
   shows it **Paused at step 1, 0% weight**, stable still serving 100%.
3. `… promote` → advances to 5%; canary pods now ~5% of the fleet; pauses again.
   Repeat → 20 → 50 → 100. No step self-advances.
4. `… abort` mid-canary → traffic/pods snap back to stable, canary RS scaled to 0.
5. consumer-worker rollout: a new image processes only the canary-fraction of
   PIX messages until promoted; **KEDA still scales the Rollout** (ScaledObject
   re-pointed) — flood PIX, the Rollout (not a Deployment) scales 0→N.
6. (If GitHub gate used) the promote workflow blocks on the `canary-5` environment
   reviewer approval before running `promote`.

---

## Track 5 — Cross-Cutting Adjustments

### Config

New env vars consolidated (ingestion-api): `RATE_LIMIT_ENABLED`,
`RATE_LIMIT_RATE`, `RATE_LIMIT_BURST`, `TRUSTED_PROXIES` (Track 1). Document all
in `.env.example` and the README env tables. No new consumer-worker env (Timescale
routing is internal; `PAYMENT_QUEUE` unchanged).

### Docker Compose

- Postgres image → `timescale/timescaledb:latest-pg17` (Track 2).
- Add `prometheus`, `grafana`, `postgres_exporter`; enable RabbitMQ Prometheus
  plugin + expose `15692` (Track 3).
- `ingestion-api` env gains the `RATE_LIMIT_*` / `TRUSTED_PROXIES` defaults
  (Track 1); locally `TRUSTED_PROXIES` is empty (no ALB → direct `RemoteAddr`).
- A `make observability-up` convenience target (compose subset: app + prom +
  grafana) and a doc line for the Grafana URL/creds.

### Helm / Pulumi

- Chart: ingestion-api `Service` → `ClusterIP` + new ALB `Ingress` (Track 1);
  `Deployment` → `Rollout` (gated by `canary.enabled`) with `trafficRouting.alb`
  on the ingestion-api side, ScaledObject re-pointed, `canary.steps` in values
  (Track 4).
- Pulumi: `albcontroller.go` + `waf.go` (Track 1), `observability.go` (Track 3),
  `argorollouts.go` (Track 4) — all wired in `main.go`; `workloads.go` drops the
  NLB override for the ALB Ingress + WAF ACL; RDS parameter-group decision for
  `timescaledb` (Track 2).

### Tests

| Area | Test |
|---|---|
| Leaky bucket (unit) | admit-burst-then-leak rate, `Retry-After` math, per-key isolation, idle eviction |
| Rate-limit middleware (unit/integration) | `c.ClientIP()` keying via `SetTrustedProxies`, spoofed-XFF ignored, `/healthz` exempt, `429` headers |
| Timescale routing (integration) | PIX insert lands in `payments_pix`; `payments` view unions; redelivery dedups on `(source_message_id, occurred_at)` — **the regression guard** |
| Timescale migration (integration) | hypertables created, 1-day chunks, idempotent re-run |
| Chart render (CI `helm lint`/`helm template`) | `canary.enabled=true` renders `Rollout` with the exact 0/5/20/50/100 steps; `=false` renders a `Deployment`; ScaledObject targets the Rollout |

### Docs

- `README.md` — add: edge-protection section (NLB→ALB front door, AWS WAF
  rate-based + managed rules, Shield Standard, the in-process IP leaky bucket +
  env + 429 semantics); TimescaleDB section (per-method hypertables, daily chunks,
  the `occurred_at` dedup decision); Grafana section (the three dashboards + how to
  reach Grafana); canary section (the 5 steps, manual promotion via
  `kubectl argo rollouts promote` / GitHub Environments). Update the architecture
  diagram to show the ALB/WAF edge, Prometheus/Grafana, and the canary rollout.
- `CLAUDE.md` — add the Phase 4 plan link; add `observability/`, the rate-limiter
  package, and `infra/pulumi/{albcontroller,waf,argorollouts,observability}.go` to
  "Where things live"; note the front door is now an **ALB Ingress + WAF** (not the
  Phase 3 NLB) and ingestion-api reads the client IP from XFF via trusted proxies;
  document the `occurred_at` dedup-key rule (it changes the Phase 1 "dedup via
  `UNIQUE` on `source_message_id`" statement — call out the two-column key);
  document the canary step convention + `canary.enabled`; note Postgres image is
  now TimescaleDB.

---

## Things to keep in mind across all tracks

1. **Dependency rule holds.** Rate-limiter → HTTP adapter; per-method table
   routing → persistence adapter; ALB/WAF edge, Grafana/Prometheus, Argo Rollouts
   → pure infra (compose/chart/Pulumi). No framework or infra concept leaks into
   `domain`/`usecase`.
2. **The `occurred_at` dedup key is the single most dangerous change.** Timescale
   forces the partition column into the unique index; partitioning by `created_at`
   would silently break idempotency on redelivery. Partition by `occurred_at`,
   dedup on `(source_message_id, occurred_at)`, and keep a regression test that
   publishes the same message twice.
3. **No separate inbox table** — dedup stays a constraint on the payments
   table(s) (CLAUDE.md rule preserved), just two-column now.
4. **`canary.enabled` gates the Rollout** so the chart still works where Argo
   Rollouts isn't installed (local compose, bare clusters). Default false locally,
   true on EKS.
5. **Re-point KEDA ScaledObjects from Deployment to Rollout** when converting the
   consumer-worker template — easy to miss, breaks autoscaling silently.
6. **Manual approval is the requirement** — bare `pause: {}` steps, not Flagger's
   auto-promotion. Automated Prometheus analysis is an *optional* safety net on
   top, never the gate.
7. **In-memory rate limiting is per-pod** — with N ingestion-api replicas the
   effective per-key limit is N×r. Document it; Redis-backed shared buckets are
   the production fix (out of scope, interface left in place for it).
8. **Grafana is internal-only in cloud** — only ingestion-api is public (Phase 3
   Track 4 boundary). Reach Grafana via port-forward / internal LB.
9. **Run `make lint`** after every Go change (rate-limiter, persistence routing) —
   zero issues before done, per the standing rule.
10. **RDS + TimescaleDB is the one cloud open question** — resolve the extension
    availability (RDS parameter group vs self-managed Timescale on EKS vs
    Timescale Cloud) at implementation time; local dev is unblocked by the
    Timescale compose image immediately.
11. **One dashboard source of truth** — the same committed dashboard JSON serves
    compose Grafana and cloud Grafana (via labeled ConfigMaps), so they never
    drift.
```
