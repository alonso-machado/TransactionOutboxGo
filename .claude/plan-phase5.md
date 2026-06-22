# Plan Phase 5 — Production Hardening: Resilient Delivery · Operability Loop · Secrets, Compliance & DR

> **When to execute:** after Phase 4 is complete and the stack runs end-to-end
> with the ALB/WAF edge, the IP leaky-bucket limiter, TimescaleDB per-method
> hypertables, Grafana/Prometheus dashboards and Argo Rollouts canaries in place.
> **Prerequisite:** `.claude/plan-phase4.md` implemented; `make up`,
> `make test-integration` and the two GitHub Actions workflows all green.

---

## Overview

Phases 1–4 built a *correct* and *observable* outbox. Phase 5 makes it
*operable under failure* — the gaps that only show up in production: tight retry
loops, a backlog metric that lies, no way to replay a dead message, schema
changes applied as boot side-effects, traces and logs with no backend, dashboards
that visualize but never page, secrets in plaintext, and no documented recovery
story.

**Explicitly out of scope (deliberate):** webhook **HMAC / signature
authentication** on `ingestion-api`. This is a showcase/demo — requiring every
`POST /api/v1/payments` to carry a valid provider signature would break
`make seed`, the k6 suites, and the "curl it and watch it flow" demo loop. The
trust boundary stays the Phase 4 WAF + IP leaky bucket. (If this ever becomes a
real system, signature verification is the first thing to add — noted in the
Security track as the documented next step, not built here.)

Six tracks, ordered so each builds the substrate the next one uses:

| # | Track | Why this order |
|---|---|---|
| 1 | **Versioned schema migrations** (replace `AutoMigrate`/raw-SQL-on-boot) | Everything else that touches the schema (backoff column, envelope version, backlog index) needs a real migration tool *first*, so the later columns ship as reviewed, versioned migrations rather than more boot-time `AutoMigrate` |
| 2 | **Resilient delivery: retry backoff · accurate backlog · DLQ replay · envelope versioning** | The core outbox/consumer reliability work; uses Track 1's migration tool to add `next_retry_at` + the backlog index; its *true* backlog metric is what Track 4 alerts on |
| 3 | **Dispatch latency & connection scaling: `LISTEN/NOTIFY` + RDS Proxy/PgBouncer** | Data-path scaling layered on the now-correct delivery loop; `NOTIFY` removes the 500 ms poll floor, pooling absorbs the KEDA fan-out |
| 4 | **The operability loop: structured logs (Loki) · traces (Tempo) · alerting/SLOs** | Observability completion — consumes Track 2's accurate metrics and needs every service emitting structured logs/traces first |
| 5 | **Secrets, compliance & DR: External Secrets · PCI posture · backup/DR runbook** | Deployment hardening; orthogonal to the app code, sits last because it touches Pulumi/chart/docs, not the hot path |
| 6 | **Cross-cutting** | Config, compose, Helm/Pulumi, tests, docs that span the tracks |

**Guiding principle (unchanged):** the Clean Architecture dependency rule holds.
`internal/domain` and `internal/usecase` never import Gin, GORM or `amqp091-go`.
New work respects this: backoff logic lives behind the existing repository ports,
the `LISTEN/NOTIFY` listener is infrastructure that *signals* the use-case via a
channel (the use-case never imports `pgx`/`lib/pq`), structured logging uses
`log/slog` (stdlib — allowed anywhere), and secrets/observability backends are
pure infra (compose + chart + Pulumi).

---

## Track 1 — Versioned Schema Migrations (replace `AutoMigrate` + boot-time raw SQL)

### Goal

Stop mutating the production schema as an implicit boot side-effect. Today
`ingestion-api` runs `AutoMigrate(&OutboxMessageModel{})` **and**
`MigrateTimescale(db)` (raw, idempotent DDL) on every start
(`internal/adapter/persistence/migrate.go`). That's fine for a demo but is the
classic production foot-gun: no version history, no review of the exact DDL, no
down-migration, and a column rename via `AutoMigrate` can silently add-without-
removing or lock a hot table.

Move to **explicit, versioned, reviewed migrations** with up/down files, applied
by a tool — not by GORM reflection.

### Decision: which tool — **golang-migrate** (run as a container)

Adoption checked (June 2026): **golang-migrate** is the most-used Go migration
tool (~18.6k GitHub stars, imported by ~5.2k projects), ahead of **goose**
(~10.4k). Mapping to the tools the team has used: **golang-migrate ≈ Flyway**
(versioned, imperative SQL `up`/`down`, CLI-first, language-agnostic); **Atlas ≈
Liquibase** (declarative desired-state + diffing).

**Chosen — golang-migrate, run as the `migrate/migrate` container** (not embedded
in the app binary), with versioned `.up.sql`/`.down.sql` pairs in a `migrations/`
directory. Why this over the alternatives *here*:

- It's the **Flyway-shaped** workflow the team already likes, but **Go-native and
  the most-adopted** Go option — and unlike **Flyway Community it ships real
  reversible `down` migrations** (Flyway gates `undo` behind the paid Teams tier).
- Running it **as a container** keeps schema migration a first-class, language-
  agnostic step that matches the project's "**Go is not on the host — everything
  runs in Podman**" model (CLAUDE.md). No migration logic compiled into the app,
  no `//go:embed`.
- Our migrations are **hand-written raw TimescaleDB DDL** (`CREATE EXTENSION`,
  `create_hypertable()`, partial indexes) — exactly the case where Atlas's
  declarative diffing gives up, so the Liquibase-style model buys little.

| Option | Verdict |
|---|---|
| **golang-migrate (containerized `migrate/migrate`)** | **CHOSEN** — most-used Go tool; Flyway-like versioned `.up.sql`/`.down.sql`; **real reversible down migrations**; runs as a `make` target locally + a pre-deploy K8s `Job`/initContainer in cloud; the existing Timescale raw DDL ports almost verbatim; a `schema_migrations` table + a Postgres advisory lock make concurrent boots safe |
| `goose` | Close second (~10.4k stars), lighter; supports Go-function migrations (unneeded — ours are all SQL). Fine fallback, just less widely adopted |
| `Atlas` (≈ Liquibase) | The declarative/diffing model the team would recognize from Liquibase; rejected because hand-written Timescale hypertable DDL fights desired-state diffing — note as the path if declarative schema management is wanted later |
| Flyway / keep `AutoMigrate` | Flyway: Java tool, and **Community has no `down` migrations** (paid Teams feature); `AutoMigrate`: the thing this track removes |

### Where migrations run (a dedicated step, not the app)

Migrations stop being something the app does at boot. Instead:

- **Local:** a `make migrate` target runs the `migrate/migrate` container against
  the compose Postgres (`migrate -path /migrations -database $DATABASE_URL up`),
  and `make up` depends on it (or a one-shot `migrate` service in compose).
- **Cloud:** a **pre-deploy K8s `Job`** (or an `initContainer` on the ingestion-api
  pod) runs `migrate ... up` **before** any app pod starts, so the schema is
  correct before the Phase 4 Rollout flips traffic. The app image no longer carries
  migration logic at all.

> **Concurrency note:** golang-migrate takes a Postgres **advisory lock**, so even
> if the migrate Job and a racing replica both try, only one applies — the other
> waits then no-ops. Safe under the HPA/Rollout.

### Migration content (port what exists, then add the new columns)

1. `000001_init.up.sql` / `.down.sql` — `outbox_messages` table (today's
   `OutboxMessageModel` shape) + its indexes (`idempotency_key` unique, `status`).
2. `000002_timescale.up.sql` / `.down.sql` — the **exact** current
   `MigrateTimescale` body (extension, 5 per-method hypertables, `event_id`
   indexes, **plus** the `payments` UNION-ALL view). Already idempotent raw SQL —
   lift it straight out of `migrate.go`; the `.down.sql` drops the view + tables.
3. `000003_outbox_backoff.up.sql` / `.down.sql` — Track 2's new columns/indexes
   (see below).

Every `.up.sql` has a real `.down.sql` (drop in reverse), so a bad deploy can roll
the schema back, not just the image. A future change is a new `000004_*.sql` pair,
never an edit to a shipped version.

### Files to add / modify

| File | Change |
|---|---|
| `migrations/000001_init.{up,down}.sql`, `000002_timescale.{up,down}.sql`, `000003_outbox_backoff.{up,down}.sql` *(new)* | Versioned SQL pairs, lifted from the current `migrate.go` then extended |
| `internal/adapter/persistence/migrate.go` | Drop `AutoMigrate`/`MigrateTimescale` (the app no longer migrates); keep `tableFor` (still used by the repo) |
| `cmd/ingestion-api/main.go` | Remove the migrate-on-boot call |
| `docker-compose.yml` | A one-shot `migrate` service (`migrate/migrate` image, `up`) the app depends on |
| `Makefile` | `make migrate` / `make migrate-down` (runs `migrate/migrate` against compose Postgres) |
| `helmcharts/.../templates/.../migrate-job.yaml` *(new)* | Pre-deploy `migrate/migrate` `Job`/initContainer; Rollout waits on it |
| `tests/integration/suite_test.go` | `TestMain` applies the `migrations/` dir via golang-migrate against the TestContainers Postgres instead of `AutoMigrate`/`MigrateTimescale` |
| CI (`.github/workflows/*.yml`) | Add a step that runs `up` → `down` → `up` against a throwaway Postgres service to prove both directions apply cleanly |

### Verification

1. `make migrate` (or `make up`) → golang-migrate applies 1→3 once; `SELECT * FROM
   schema_migrations` shows version 3; re-running is a no-op.
2. The migrate Job and a booting replica race → exactly one applies (advisory
   lock), no error.
3. CI runs `up` → `down` → `up` on a clean Postgres → green (the down migrations
   are real, not stubs).
4. Integration suite passes with no `AutoMigrate` anywhere.

---

## Track 2 — Resilient Delivery: Backoff · Accurate Backlog · DLQ Replay · Envelope Versioning

The reliability heart of Phase 5. Four related fixes; all but the last are
provably-needed from the current code.

### 2.A — Retry backoff (today retries are a tight loop on **both** sides)

**Outbox side (the bug):** `DispatchOutbox.dispatch` re-fetches `RETRYING` rows
every `OUTBOX_DISPATCH_INTERVAL_MS` (default **500 ms**) with no delay gate —
`FetchPending` selects `status IN (NEW, RETRYING)` ordered by `created_at`
(`internal/adapter/persistence/outbox_repo.go:45`), and `MarkRetrying` only bumps
`retry_count`. So a message failing because RabbitMQ is unreachable is retried
**twice a second** until it exhausts `OUTBOX_MAX_RETRIES` (5) and dead-letters —
a hot loop against a struggling broker, exactly when you want to back off.

**Fix:** add a `next_retry_at timestamptz` column.
- `MarkRetrying` sets `next_retry_at = now() + backoff(retry_count)` where
  `backoff` is **exponential with full jitter**, e.g.
  `min(base * 2^retry_count, cap)` with `base=1s`, `cap=5m`, jittered.
- `FetchPending` gains `AND (next_retry_at IS NULL OR next_retry_at <= now())`
  so `NEW` rows (null) fire immediately and `RETRYING` rows wait out their
  backoff. The `FOR UPDATE SKIP LOCKED` ordering is unchanged.
- New partial index `(next_retry_at) WHERE status IN ('NEW','RETRYING')` keeps
  the fetch cheap as the table grows.

**Consumer side:** `requeueWithRetryCount` republishes the failed delivery to the
**same queue immediately** (`internal/adapter/messaging/consumer.go:180`), so a
message that fails on a transient DB blip is re-processed as fast as prefetch
allows, burning through `MAX_DELIVERIES` (5) in milliseconds.

**Fix — RabbitMQ-native delayed retry:** declare a per-method **retry queue**
`payments.<method>.retry` with **no consumer**, a **dead-letter exchange back to
the main queue**, and a **per-message TTL** = `backoff(retryCount)` (set via the
`expiration` field on republish). The message sits in the retry queue for its
backoff, then the broker dead-letters it back onto `payments.<method>.queue` for
another attempt. Variable per-message TTL gives true exponential backoff without
a plugin.

| Option | Verdict |
|---|---|
| **Per-method retry queue + per-message TTL + DLX back to main** | **CHOSEN** — no plugin, classic AMQP, backoff carried per message via `expiration`. Topology declaration already loops over `rmq.Methods` so it's one more queue per method |
| `rabbitmq_delayed_message_exchange` plugin | Cleaner API but needs a plugin installed on the broker (and Amazon MQ doesn't allow arbitrary plugins) — reject for portability |
| Keep immediate requeue | Rejected — the hot loop this section exists to fix |

> Keep the existing `x-retry-count` header bookkeeping — it already drives the
> poison→DLQ decision; backoff just adds the delay between attempts.

### 2.B — Fix the lying backlog gauge

`DispatchOutbox.dispatch` records `d.pendingCount.Record(ctx, int64(len(msgs)))`
(`internal/usecase/outbox/dispatch.go:126`) — but `msgs` is **capped at
`batchSize` (50)**. A 10,000-row backlog reads as a flat **50**. This is the one
metric you'd autoscale and alert on, and it's blind above the batch size.

**Fix:** add `OutboxRepository.CountPending(ctx) (int64, error)` →
`SELECT count(*) WHERE status IN ('NEW','RETRYING')` (uses the partial index from
2.A). The dispatcher records the *true* count each tick. Cheap, and now Track 4
can alert "backlog rising for 5 min".

> Also expose `outbox.dead_letter_count` as a gauge (count of `DEAD_LETTER` rows)
> — a non-zero value is an operator signal Track 4 pages on.

### 2.C — DLQ / dead-letter replay tooling

Today a poison message rejects to the RabbitMQ `.dlq` and the outbox row lands in
`DEAD_LETTER`, and then... nothing. There's no supported way to inspect or
**replay** either. Production needs both.

**Two replay paths, mirroring the two dead-letter sinks:**

1. **Outbox `DEAD_LETTER` → `NEW` requeue.** A maintenance operation that resets a
   dead outbox row: `status = NEW`, `retry_count = 0`, `next_retry_at = NULL`,
   clears `last_error`. The existing dispatch loop picks it up and re-publishes.
2. **RabbitMQ `.dlq` → main-queue drain.** Move messages from
   `payments.<method>.dlq` back onto `payments.<method>.queue` (resetting
   `x-retry-count`), for poison messages that failed on the *consumer* side after
   a fix is deployed.

**Delivery mechanism — not a new long-running binary.** CLAUDE.md forbids adding a
third binary *for dispatch*; this is a short-lived maintenance command, which is
different, but to stay safe and explicit:

| Option | Verdict |
|---|---|
| **A small admin command run as a one-shot K8s `Job` / `make` target** (e.g. `go run ./cmd/outbox-admin replay-dead --method PIX --limit 100`) | **CHOSEN** — ephemeral, not a third *service*; auditable; runs with the same image/config. Doesn't violate the "dispatch is a goroutine, not a process" rule |
| Internal admin HTTP port on ingestion-api (separate listener, **never** behind the ALB) | Acceptable alternative; document the bind-to-internal-only requirement. More moving parts |
| Public admin endpoint | **Rejected** — ingestion-api is the only public front door (Phase 3/4 boundary); replay must not be reachable from the edge |

Whichever is chosen, the replay logic itself lives behind the existing
`OutboxRepository` / a new small `DLQReplayer` port so it's unit-testable and
framework-free.

### 2.D — Message envelope versioning

The outbox payload (`ingest.outboxPayload`) and the consumer DTO
(`consume.payloadDTO`) share a hand-kept shape with **no version field**. Today
any change to that contract is a silent coordinated deploy; one schema drift
between a new producer and an old consumer = a parse error → poison → DLQ.

**Fix:** add `"schemaVersion": "1"` to the outbox payload and the RabbitMQ message
(as a header **and** in the body). The consumer reads it first:
- **Same major version** → process as today.
- **Unknown/newer major version** → don't crash-loop; record the version on the
  span, increment a `consumer.unknown_schema_version_total` metric, and reject to
  DLQ (operator signal) rather than retrying forever.

This is cheap now and impossible to retrofit cleanly later — it's the seam that
lets the producer/consumer contract evolve independently (and lets a future 6th
method or field land without a lockstep deploy).

### Files to add / modify (Track 2)

| File | Change |
|---|---|
| `migrations/V3__outbox_backoff.sql` | `ALTER TABLE outbox_messages ADD COLUMN next_retry_at timestamptz`; partial index `(next_retry_at) WHERE status IN ('NEW','RETRYING')` |
| `internal/domain/outbox.go` | `OutboxMessage.NextRetryAt *time.Time`; add `CountPending(ctx) (int64, error)` to `OutboxRepository`; add a `DLQReplayer`/replay method |
| `internal/adapter/persistence/outbox_repo.go` | `FetchPending` adds the `next_retry_at <= now()` predicate; `MarkRetrying` sets `next_retry_at = now()+backoff`; implement `CountPending`, dead-letter→new requeue |
| `internal/usecase/outbox/dispatch.go` | Record `CountPending` (not `len(msgs)`) into `pending_count`; add `dead_letter_count` gauge; compute backoff schedule |
| `internal/infrastructure/rabbitmq/rabbitmq.go` | Declare per-method `*.retry` queues (TTL/DLX back to main) in `DeclareTopology`/`DeclareQueue` |
| `internal/adapter/messaging/consumer.go` | `requeueWithRetryCount` → publish to the `*.retry` queue with `expiration = backoff(retryCount)` instead of immediate same-queue republish |
| `internal/usecase/ingest/ingest.go` | Add `SchemaVersion` to `outboxPayload`; set the constant |
| `internal/usecase/consume/process.go` | Read/validate `schemaVersion`; unknown-version → DLQ + metric |
| `cmd/outbox-admin/main.go` *(new)* OR internal admin handler | Replay commands (dead→new, dlq→queue) |
| `internal/infrastructure/config/config.go` | `RetryBackoffBase`, `RetryBackoffCap` durations |

### Verification (Track 2)

1. Stop RabbitMQ, POST a payment → the outbox row goes `RETRYING`; observe
   `next_retry_at` stepping out exponentially (≈1s, 2s, 4s…), **not** retried
   every 500 ms. Restart RabbitMQ → it publishes on the next due tick.
2. Enqueue 5,000 rows with RabbitMQ down → `outbox.pending_count` reads **5000**,
   not 50.
3. Consumer-side: make `ProcessMessage` fail transiently → the message lands in
   `payments.pix.retry`, waits its TTL, dead-letters back to `payments.pix.queue`,
   and succeeds on retry; exhausting `MAX_DELIVERIES` → `.dlq`.
4. Force a row to `DEAD_LETTER`; run the replay Job → row returns to `NEW` and
   publishes; drain a `.dlq` → messages re-appear on the main queue.
5. Publish a message with `schemaVersion: "99"` → consumer rejects to DLQ,
   `unknown_schema_version_total` ticks, **no crash-loop**.

---

## Track 3 — Dispatch Latency & Connection Scaling

### 3.A — `LISTEN/NOTIFY` to remove the 500 ms publish-latency floor

The dispatcher polls every `OUTBOX_DISPATCH_INTERVAL_MS` (500 ms), so *every*
payment waits up to 500 ms between commit and publish even on an idle system.
Add a Postgres **`NOTIFY`** so enqueue wakes the dispatcher immediately; keep the
poll as a safety net (covers missed notifications, multi-replica, and `RETRYING`
rows coming due).

**Clean-Architecture-safe wiring (important):**
- `Enqueue` issues `pg_notify('outbox_new', '')` inside the same transaction (a
  trigger on `INSERT`, or an explicit `NOTIFY` in the repo — repo is the adapter
  layer, so `pg_notify` is fine there).
- A small **infrastructure** listener (`pgx`/`lib/pq` `LISTEN` on a *dedicated
  connection* — GORM doesn't expose `LISTEN`) receives notifications and pushes to
  a `chan struct{}`.
- `DispatchOutbox.Run` selects over `ticker.C`, `pruneTicker.C`, **and** the new
  `trigger` channel — debounced so a burst of NOTIFYs coalesces into one dispatch.
  The use-case imports **no** pgx; it just receives from a channel handed in by
  `main.go`. Dependency rule intact.

> Keep it strictly an *optimization*: if the listener connection drops, the
> 500 ms poll still drains everything — correctness never depends on NOTIFY.

### 3.B — Connection pooling for the KEDA fan-out

Phase 3/4 scale consumers **0→N per method across 5 methods** via KEDA. Each pod
opens its own Postgres pool; at peak that's `5 × maxReplicas × poolSize`
connections — easily past RDS `max_connections`. Ingestion-api's HPA adds more.

**Fix:** put **PgBouncer** in front of Postgres — the same pooler runs in compose
**and** on EKS, so local and cloud behave identically and the pooler itself is
observable (3.C / Track 4 add its metrics to the INFRA dashboard).

| Option | Verdict |
|---|---|
| **PgBouncer** (compose service + EKS Deployment, `transaction` mode) | **CHOSEN — everywhere** — one lightweight container; `transaction` pooling collapses the KEDA fan-out's many short-lived sessions onto a small server-connection pool; identical local/cloud behavior; exposes admin stats (`SHOW STATS`/`SHOW POOLS`) that a `pgbouncer-exporter` turns into Prometheus metrics for the INFRA dashboard |
| **AWS RDS Proxy** (managed alternative, cloud only) | Noted, not chosen — managed + IAM-auth + failover-aware, but it's AWS-only (no local equivalent, so local/cloud diverge) and its CloudWatch metrics wouldn't land on the same Prometheus/Grafana INFRA dashboard. Document it as the drop-in swap for teams that prefer fully-managed |
| Nothing (rely on pool caps) | Rejected — KEDA fan-out is exactly the scenario that exhausts `max_connections` |

> **Caveat to honor, not hide:** PgBouncer `transaction` mode is incompatible with
> server-side prepared statements unless pinning is handled. **Disable GORM's
> `PrepareStmt`** (or set the `pgx` `statement_cache_mode` appropriately) so the app
> doesn't break behind the pooler. Verify the outbox `FOR UPDATE SKIP LOCKED`
> transaction still behaves (it does — it's a single transaction, no cross-statement
> session state). PgBouncer ≥ 1.21 also has native prepared-statement support in
> transaction mode if `PrepareStmt` is wanted later.

### 3.C — Make PgBouncer observable (`pgbouncer-exporter`)

PgBouncer doesn't speak Prometheus natively — it exposes stats through its admin
pseudo-database (`SHOW STATS`, `SHOW POOLS`, `SHOW LISTS`, `SHOW DATABASES`). Add
the community **`pgbouncer-exporter`**
(`quay.io/prometheuscommunity/pgbouncer-exporter`) connected to PgBouncer's admin
DB; Prometheus scrapes it on `:9127`, and the metrics render on the **INFRA Grafana
dashboard** (Track 4 / 4.D) right next to the existing Postgres panels. The key
signal is **`cl_waiting` / `maxwait`** — clients queued for a server connection,
which is the direct "the pool is too small for the current KEDA fan-out" alarm.

### Files to add / modify (Track 3)

| File | Change |
|---|---|
| `migrations/000004_outbox_notify.{up,down}.sql` | `INSERT` trigger calling `pg_notify('outbox_new', '')` (or do the NOTIFY in repo) |
| `internal/infrastructure/database/listener.go` *(new)* | `pgx`/`lib/pq` `LISTEN outbox_new` on a dedicated conn → `chan struct{}` |
| `internal/usecase/outbox/dispatch.go` | `Run` selects over a `trigger <-chan struct{}` (debounced) in addition to the tickers |
| `cmd/ingestion-api/main.go` | Start the listener, wire its channel into `DispatchOutbox`; disable `PrepareStmt` |
| `internal/infrastructure/database/database.go` | `PrepareStmt: false` (pooler-safe); honor pooled `DATABASE_URL` |
| `docker-compose.yml` | Add `pgbouncer` (transaction mode) in front of Postgres + `pgbouncer-exporter`; app + consumers `DATABASE_URL` → pgbouncer |
| `helmcharts/.../templates/pgbouncer/*` *(new)* | PgBouncer Deployment + Service (+ exporter sidecar) on EKS; app/consumer `DATABASE_URL` → the PgBouncer Service |
| `infra/pulumi/data.go` | (Optional/alt) `rds.Proxy` documented as the managed swap for PgBouncer |
| `observability/pgbouncer/pgbouncer.ini`, `userlist.txt` *(new)* | PgBouncer config (transaction mode, admin user for the exporter) |

### Verification (Track 3)

1. Idle system: POST a payment → it publishes in **single-digit ms**, not ~250 ms
   average (NOTIFY path). Kill the listener connection → still publishes within
   500 ms (poll fallback).
2. k6 autoscale run with KEDA fanning consumers out → Postgres
   `numbackends`/`max_connections` (infra dashboard) stays bounded by the pooler,
   not linear in pod count.
3. App works correctly behind the pooler (no "prepared statement already exists"
   errors) — `PrepareStmt` disabled.

---

## Track 4 — The Operability Loop: Structured Logs · Trace Backend · Alerting/SLOs

Phase 4 stood up Grafana + Prometheus and dashboards, but the loop is only half
closed: logs are unstructured, traces land in a *separate* UI (Jaeger) instead of
correlating with metrics/logs in Grafana, and nothing pages.

### 4.A — Structured logging (`log/slog`) + Loki

Today everything is `log.Printf(...)` (every adapter, use-case, `main`). That's
unparseable in production — you can't filter by `payment_method`, correlate to a
trace, or alert on a log pattern.

**Fix:** switch to **`log/slog`** with a **JSON handler** (stdlib — no framework,
allowed in any layer including use-cases). Standard fields on every line:
`service`, `level`, `msg`, plus `trace_id`/`span_id` pulled from the context span
(a small `slog.Handler` wrapper reads the active `trace.SpanContext`). Then logs
join traces in Grafana.

Replace ad-hoc `log.Printf("... %s", pii.Redact(err.Error()))` with
`slog.ErrorContext(ctx, "outbox publish failed", "method", msg.PaymentMethod,
"err", pii.Redact(err.Error()))` — **keep the existing PII redaction**; it just
moves into structured fields.

**Shipping:** containers log JSON to stdout; **Grafana Loki** + **Promtail/Grafana
Alloy** scrape it. A Grafana *derived field* turns `trace_id` in a log line into a
link to the trace in Tempo (4.B). One correlated view: metric spike → trace →
the exact redacted log line.

### 4.B — Trace backend: Jaeger → Grafana Tempo (close the correlation loop)

Phase 2 emits OTLP traces and compose **already runs Jaeger** as the receiver
(`OTEL_EXPORTER_OTLP_ENDPOINT=jaeger:4318`, Jaeger UI on `:16686`) — so traces *do*
land locally. The real gaps: (1) the Go config default is still bare
`localhost:4318` (works only because compose overrides it), (2) there is **no
trace backend in cloud**, and (3) Jaeger is a *separate* UI — it doesn't give the
Grafana-native **trace ↔ logs ↔ metrics** correlation (click a metric exemplar or
a log's `trace_id` → jump to the span) that closes the operability loop.

**Fix:** swap Jaeger for **Grafana Tempo** so traces live in the same Grafana as
the Phase 4 dashboards and the Track 4.A Loki logs.
- Compose: replace the `jaeger` service with a `tempo` service; keep
  `OTEL_EXPORTER_OTLP_ENDPOINT` pointed at it (`tempo:4318`). Grafana gets a Tempo
  datasource; wire exemplars (histogram → Tempo) and the `trace_id` derived field
  (Loki → Tempo).
- Cloud: Tempo via the `tempo`/`tempo-distributed` Helm chart (or Grafana Cloud /
  AWS X-Ray as the documented managed alternative), wired into the Phase 4
  `kube-prometheus-stack` Grafana as a datasource.

> If the team prefers to **keep Jaeger** (it's already working locally), that's a
> valid lighter option — but then trace↔metric↔log correlation stays manual and
> cloud still needs a deployed Jaeger. Tempo is recommended specifically for the
> single-pane Grafana correlation.

### 4.C — Alerting + SLOs (dashboards that page, not just show)

Phase 4 dashboards visualize; nothing fires. Add **`PrometheusRule`** alerts
(kube-prometheus-stack consumes them in cloud; a Prometheus rule file +
**Alertmanager** service locally) and define **SLOs**.

**SLOs (the targets the alerts defend):**
- `ingestion-api` availability: **99.9%** of `POST /api/v1/payments` non-5xx.
- End-to-end outbox latency: **p99 enqueue→published < N s** (from the new
  accurate metrics).

**Alerts (each ties to a concrete Phase 5 signal):**

| Alert | Condition | Sourced from |
|---|---|---|
| `OutboxBacklogGrowing` | `outbox.pending_count` rising > 5 min | 2.B (the fixed gauge) |
| `OutboxDeadLetterPresent` | `outbox.dead_letter_count > 0` | 2.B |
| `RabbitMQDLQDepth` | any `payments.<method>.dlq` depth > 0 | Phase 4 infra dashboard |
| `IngestErrorBudgetBurn` | multi-window 5xx burn-rate vs the 99.9% SLO | otelgin histogram |
| `ConsumerPoisonRate` | `messages_processed_total{outcome="poison_dlq"}` rate spike | Phase 3 consumer metrics |
| `UnknownSchemaVersion` | `unknown_schema_version_total > 0` | 2.D |
| `RateLimitRejectSpike` | `ingestion.ratelimit_rejected_total` surge | Phase 4 Track 1 |

Use **multi-window, multi-burn-rate** alerting for the SLO ones (fast burn pages,
slow burn tickets) — the Google SRE pattern — so on-call isn't paged on noise.

> Routing: Alertmanager → a documented sink (Slack/PagerDuty/email). For the
> showcase, wire a single Slack/webhook receiver and document swapping it.

### 4.D — PgBouncer on the INFRA Grafana dashboard

The INFRA dashboard already exists (`observability/grafana/dashboards/infra.json`,
Phase 4 — RabbitMQ + Postgres/Timescale panels). Track 3 added PgBouncer + its
`pgbouncer-exporter`; this folds the pooler's health into that **same** dashboard,
right beside the Postgres panels, so "is the pool the bottleneck?" is answerable at
a glance during a KEDA fan-out.

**Wiring:** add a Prometheus scrape job for the exporter, then extend `infra.json`
with a new **"PgBouncer"** row:

```yaml
# observability/prometheus/prometheus.yml — new job (sits next to the postgres job)
- job_name: pgbouncer
  static_configs:
    - targets: ["pgbouncer-exporter:9127"]
      labels: { service: pgbouncer }
```

**Panels (the metrics `pgbouncer-exporter` exposes from `SHOW STATS`/`SHOW POOLS`):**

| Panel | Metric(s) | Why it matters |
|---|---|---|
| **Clients waiting for a connection** | `pgbouncer_pools_client_waiting_connections` (`cl_waiting`) | **The headline tile** — non-zero means the pool is too small for the current KEDA fan-out; the direct "scale the pool / raise `default_pool_size`" signal |
| **Max client wait time** | `pgbouncer_pools_client_maxwait_seconds` (`maxwait`) | How long the unluckiest client queued — pairs with the alert below |
| **Server connections** | `pgbouncer_pools_server_active_connections` / `_idle_` / `_used_` | Actual upstream Postgres connections — shows the multiplexing win (many clients → few server conns) |
| **Client connections** | `pgbouncer_pools_client_active_connections` | Inbound load from app + the 5×N consumers |
| **Pooling efficiency** | client-active ÷ server-active | The collapse ratio PgBouncer buys; the justification panel for the whole pooler |
| **Avg query / wait time** | `pgbouncer_stats_*_query_time` / `_wait_time` | Latency added by pooling; should stay near zero unless saturated |

Add a matching alert to 4.C's `PrometheusRule`: **`PgBouncerClientsWaiting`** —
`pgbouncer_pools_client_waiting_connections > 0` for > 2 min (pool saturated). One
committed `infra.json` serves compose *and* cloud Grafana (Phase 4's single-source-
of-truth convention), so the PgBouncer row appears in both.

### Files to add / modify (Track 4)

| File | Change |
|---|---|
| `internal/infrastructure/logging/slog.go` *(new)* | JSON `slog` setup + trace-correlating handler wrapper |
| **all** `log.Printf` call sites (adapters, use-cases, `main`) | → `slog.*Context`, keep `pii.Redact` in structured fields |
| `observability/tempo/tempo.yaml` *(new)* | Tempo config |
| `observability/loki/*`, `observability/promtail/*` *(new)* | Loki + Promtail/Alloy config |
| `observability/prometheus/rules/*.yml` *(new)* | Alert rules above |
| `observability/alertmanager/alertmanager.yml` *(new)* | Receiver/routing |
| `observability/grafana/provisioning/datasources/*` | Add Tempo + Loki datasources; derived `trace_id` field |
| `observability/prometheus/prometheus.yml` | Add the `pgbouncer` scrape job (4.D) |
| `observability/grafana/dashboards/infra.json` | Add the **PgBouncer** row (4.D panels) to the existing INFRA dashboard |
| `docker-compose.yml` | Replace `jaeger` with `tempo`; add `loki`, `promtail`/`alloy`, `alertmanager`; OTLP → `tempo:4318` (the `pgbouncer-exporter` service is added in Track 3) |
| `infra/pulumi/observability.go` | Add Tempo + Loki Helm releases; `PrometheusRule` CRDs (incl. `PgBouncerClientsWaiting`); Alertmanager config |

### Verification (Track 4)

1. `make up` → app logs are JSON with `trace_id`; Grafana Explore (Loki) filters
   by `payment_method`; clicking a log's `trace_id` opens the trace in Tempo.
2. Tempo shows the full ingest→outbox→consume span tree for one payment.
3. Trip an alert: stop RabbitMQ, enqueue rows → `OutboxBacklogGrowing` and (after
   dead-lettering) `OutboxDeadLetterPresent` fire in Alertmanager and hit the
   configured receiver.
4. Burn-rate alert: drive 5xx via k6 → `IngestErrorBudgetBurn` fires on the fast
   window, not on a single blip.

---

## Track 5 — Secrets, Compliance & DR

Deployment hardening, orthogonal to the hot path.

### 5.A — Secrets management (External Secrets Operator → AWS Secrets Manager)

The chart ships DB/RabbitMQ creds as plaintext K8s `Secret`s
(`helmcharts/.../templates/secret.yaml`). Production wants them sourced from a
real secrets store with rotation.

- **External Secrets Operator** (Helm release via Pulumi, same pattern as
  `keda.go`) syncs **AWS Secrets Manager** secrets into K8s `Secret`s via
  `ExternalSecret` CRDs; ESO authenticates with **IRSA** (no static creds).
- Chart: the static `Secret` becomes an `ExternalSecret` gated by
  `externalSecrets.enabled` (default **false** locally → plain `Secret`/`.env`
  still works for `make up`; **true** on EKS).
- Rotation: RDS-managed credential rotation in Secrets Manager; ESO re-syncs;
  pods pick up the new secret (document the rollout/refresh interval).

> This is the one piece of the original "security" suggestion kept — it doesn't
> touch the demo's request path (purely deployment infra), unlike HMAC auth.

### 5.B — PCI-DSS posture (mostly assert + a few toggles)

It's card data, so make the posture explicit (documentation track, with the few
config toggles that back it):
- **PAN handling** — already correct: masked to last-4 before persist/publish/log
  (`card.go`, `maskPAN`) + `cardNumber` in `pii.Redact`. Assert it; add a test
  that greps the outbox payload / RabbitMQ body / logs for a full PAN and fails if
  found (regression guard).
- **No CVV — ever.** Assert it's never accepted, stored, or logged.
- **Encryption in transit** — enforce **TLS** on the RDS connection
  (`sslmode=require`) and on RabbitMQ/Amazon MQ (`amqps://`), and HTTPS on the ALB
  (ACM cert). Today local is plaintext; make TLS the cloud default via config.
- **Encryption at rest** — RDS + EBS via **KMS** (Pulumi), document key
  ownership.
- **Audit + segmentation** — note the private-subnet topology (Phase 3/4), the
  WAF, and that consumer-worker is the only writer. Document the audit-log story
  (who-saw-what) as the gap a real deployment closes.

### 5.C — Backup & disaster recovery

No recovery story exists today. Add one:
- **RDS automated backups + PITR** + **cross-region snapshot copy** (Pulumi);
  document **RPO/RTO**.
- The **outbox is the replay log**: because every payment is durably enqueued
  before publish, a RabbitMQ loss is recoverable by re-dispatching `NEW`/un-acked
  rows — call this out as the architectural DR property the whole pattern buys.
- A written **runbook**: restore-from-PITR steps, the DLQ-replay procedure (2.C),
  and the "broker rebuilt from outbox" procedure.

### Files to add / modify (Track 5)

| File | Change |
|---|---|
| `infra/pulumi/externalsecrets.go` *(new)* | ESO Helm release + IRSA |
| `infra/pulumi/data.go` | RDS KMS-at-rest, `sslmode=require`, automated backups + PITR + cross-region snapshot copy |
| `helmcharts/.../templates/externalsecret.yaml` *(new)* | `ExternalSecret` gated by `externalSecrets.enabled`; static `Secret` is the local fallback |
| `internal/infrastructure/{database,rabbitmq}/*.go` | Honor TLS (`sslmode`, `amqps://`) from config |
| `internal/infrastructure/config/config.go` | `DBSSLMode`, `RabbitMQTLS` toggles |
| `tests/integration/pci_test.go` *(new)* | Assert no full PAN / CVV in outbox payload, RabbitMQ body, or logs |
| `docs/runbook.md` *(new)* | DR / restore / DLQ-replay / broker-rebuild runbook |
| `SECURITY.md` *(new)* | PCI posture + the documented "HMAC auth is the next step if productionized" note |

### Verification (Track 5)

1. On EKS, secrets resolve via ESO from Secrets Manager; rotating the RDS secret
   re-syncs to the pods.
2. `externalSecrets.enabled=false` (local) still boots with plain `.env`/`Secret`.
3. PCI test: a card payment leaves only last-4 in the outbox payload, the RabbitMQ
   message, and the logs — the regression guard fails on any full PAN.
4. Cloud connections are TLS (`amqps`, `sslmode=require`); RDS shows KMS at rest +
   automated backups + a cross-region snapshot.
5. Runbook dry-run: restore a PITR snapshot to a scratch instance; replay a
   dead-letter row end-to-end.

---

## Track 6 — Cross-Cutting Adjustments

### Config (new env vars)

```
RETRY_BACKOFF_BASE    default "1s"       # outbox + consumer retry backoff base
RETRY_BACKOFF_CAP     default "5m"       # backoff ceiling
DB_SSL_MODE           default "disable"  # "require" in cloud
RABBITMQ_TLS          default "false"    # true (amqps) in cloud
LOG_FORMAT            default "json"     # slog handler (text for local readability if desired)
```

`OTEL_EXPORTER_OTLP_ENDPOINT` default moves from `localhost:4318` →
`tempo:4318` in compose (Track 4). Document all in `.env.example` + README tables.

### Docker Compose

Add `pgbouncer` + `pgbouncer-exporter` (Track 3); add a one-shot `migrate` service
(Track 1); **replace** `jaeger` with `tempo` and add `loki` + `promtail`/`alloy` +
`alertmanager` (Track 4). Point services' `DATABASE_URL` at PgBouncer and OTLP at
Tempo. A `make observability-up` already exists (Phase 4) — extend it to the new
services; add `make migrate` / `make replay-dead` / `make drain-dlq` convenience
targets.

### Helm / Pulumi

- Chart: migration `Job`/initContainer (T1); `pgbouncer` Deployment + Service (+
  exporter) (T3); `*.retry` queues are app-declared so no chart change there;
  `ExternalSecret` gated by `externalSecrets.enabled` (T5); `PrometheusRule`s +
  Tempo/Loki datasources + the PgBouncer INFRA-dashboard row (T4).
- Pulumi: PgBouncer (via the chart) with `rds.Proxy` documented as the managed
  alternative (T3); Tempo/Loki/ESO Helm releases (T4/T5); RDS KMS + PITR +
  cross-region copy (T5); `PrometheusRule` CRDs (T4) — all wired in `main.go`.

### Tests

| Area | Test |
|---|---|
| Migrations (CI + integration) | `up`→`down`→`up` clean; advisory-lock concurrency; suite applies the `migrations/` dir via golang-migrate (no `AutoMigrate`) |
| Backoff (unit) | exponential+jitter schedule; `FetchPending` respects `next_retry_at`; `MarkRetrying` sets it |
| Backlog metric (integration) | 5,000 pending rows → `CountPending` returns 5,000 (regression for the `len(msgs)` bug) |
| Consumer retry queue (integration) | failed message → `*.retry` → TTL → back to main → success; exhaustion → `.dlq` |
| DLQ replay (integration) | dead outbox row → `NEW` → published; `.dlq` drain → main queue |
| Envelope version (unit/integration) | unknown `schemaVersion` → DLQ + metric, no crash |
| NOTIFY (integration) | enqueue triggers near-immediate dispatch; listener-down falls back to poll |
| PgBouncer (manual/load) | app works behind the pooler (`PrepareStmt` off); under KEDA fan-out the `pgbouncer-exporter` `cl_waiting`/server-conn panels render on the INFRA dashboard |
| PII/PCI (integration) | no full PAN/CVV anywhere (T5) |
| Chart render (CI `helm lint`/`template`) | migration Job renders; `externalSecrets.enabled` toggles `ExternalSecret` ↔ `Secret` |

### Docs

- `README.md` — add: migrations section (versioned golang-migrate, `migrate
  up`/`down` via the container); resilient delivery (backoff, retry queues, DLQ
  replay, envelope version); NOTIFY + **PgBouncer** pooling (with its INFRA-
  dashboard panels); the operability loop (Loki + Jaeger→Tempo + Alertmanager, the
  SLOs + alerts); secrets/PCI/DR. Update the architecture diagram to show
  PgBouncer, Tempo/Loki/Alertmanager, ESO, and the retry queues.
- `CLAUDE.md` — add the Phase 5 plan link; record: migrations replace
  `AutoMigrate` (versioned `golang-migrate`, run as the `migrate/migrate`
  container via a `make` target / pre-deploy Job, not the app); the outbox gains
  `next_retry_at` + exponential backoff (changes the Phase 1 "RETRYING re-fetched
  each tick" behavior); the `pending_count` gauge is now a real `COUNT` not the
  batch size; per-method `*.retry` queues exist alongside the `.dlq`; the message
  envelope carries `schemaVersion`; logging is `slog` JSON (no more `log.Printf`);
  Tempo/Loki/Alertmanager are the observability backends; ESO sources secrets in
  cloud; the runbook lives at `docs/runbook.md`.

---

## Things to keep in mind across all tracks

1. **Dependency rule holds.** Backoff logic stays behind the repository ports; the
   `LISTEN/NOTIFY` listener is infrastructure that signals the use-case via a
   channel (use-case imports no `pgx`); `slog` is stdlib (allowed anywhere);
   pooling, Tempo/Loki/ESO, RDS Proxy are pure infra. No framework leaks into
   `domain`/`usecase`.
2. **The `next_retry_at` change alters Phase 1 semantics** — `RETRYING` rows are no
   longer eligible every tick; `FetchPending`'s new predicate is load-bearing.
   Keep the regression test that a failing message backs off instead of hot-looping.
3. **The backlog gauge bug is a real one-line correctness fix** — `len(msgs)` is
   capped at `batchSize`; replace with `CountPending`. Track 4's most important
   alert depends on it.
4. **Retry backoff via per-message TTL needs the `*.retry` queue + DLX** — no
   broker plugin (Amazon MQ portability); keep the `x-retry-count` header for the
   poison decision.
5. **Migrations are versioned and reviewed now** — never reach for `AutoMigrate`
   again; new columns ship as `000N_*.up.sql`/`.down.sql` pairs with a real down.
6. **Pooling breaks server-side prepared statements** — disable GORM `PrepareStmt`
   when running behind PgBouncer/RDS Proxy in transaction mode, or it fails under
   load.
7. **NOTIFY is an optimization, never correctness** — the 500 ms poll must still
   drain everything if the listener connection drops.
8. **Keep PII redaction through the slog migration** — `pii.Redact` moves into
   structured fields, it doesn't disappear; the PCI regression test guards it.
9. **HMAC auth is deliberately out of scope** — documented in `SECURITY.md` as the
   first thing to add if this stops being a demo; the request path stays open so
   `make seed`/k6/curl keep working.
10. **`externalSecrets.enabled` / `canary.enabled`-style gating** — every cloud-only
    hardening (ESO, RDS Proxy, TLS, PITR) must degrade to a working local default
    so `make up` never needs AWS.
11. **Run `make lint`** after every Go change (backoff, repo, slog migration,
    consumer retry, admin command) — zero issues before done, per the standing rule.
12. **Don't add a third *service*** — the DLQ-replay command is a one-shot Job/CLI,
    not a long-running process; dispatch stays a goroutine inside ingestion-api.
