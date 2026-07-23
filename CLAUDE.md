# CLAUDE.md — Guide for Claude Code

This file gives Claude Code context about the project so it can assist
effectively without re-deriving the same conclusions each session.

## What this project is

A Go monorepo implementing the **Transactional Outbox** pattern for an
**Event Ticket System**:

- **`ingestion-api`** — accepts `POST /api/v1/orders` (a request for tickets
  to an event), stores it durably in Postgres (the `order_outbox` table only
  — it never writes to the `events` database directly), pre-generates the
  Order UUID and embeds the full order data in the outbox payload, returns
  `201 Created`. It also serves **`POST /api/v1/webhooks/payments/{provider}`**,
  which verifies a payment-gateway webhook via the configured
  `domain.PaymentGateway` port and lands the confirmation in a second
  outbox table, `payment_event_outbox`. It does **not** talk to RabbitMQ or
  the `events` database at all (so it keeps accepting writes even when the
  broker or the domain DB is down) and runs at a **fixed 1 replica** — it's a
  thin write path in front of the two outboxes.
- **`outbox-worker`** — the Transactional Outbox relay (`DispatchOutbox`),
  run **twice** in one process (not two binaries): once for `order_outbox`
  (with a LISTEN/NOTIFY fast path) and once for `payment_event_outbox`
  (poll-only — lower volume). The only reader of either outbox table and the
  only RabbitMQ publisher: poll → dedup → publish with confirms → mark
  `PUBLISHED`/`RETRYING`/`DEAD_LETTER`, with exponential backoff + DLQ.
  Scales on the SUM of both outboxes' backlog via KEDA (postgresql scaler),
  **min 1 replica** (keeps the NOTIFY path warm and the prune/metrics tickers
  alive).
- **`order-consumer-worker`** — consumes `order_outbox` messages (routed by
  `event_type`/`event_subtype`, one shard's queue per instance): upserts the
  `Location`/`Event` the order belongs to, reserves `Ticket` rows, opens a
  checkout with the configured `PaymentGateway`, and persists a `Charge`
  (`PENDING`). Dedupes via a `UNIQUE` constraint on `orders.source_order_id`.
- **`fulfillment-consumer-worker`** — consumes `payment_event_outbox`
  messages (same routing): on a `CONFIRMED` payment it marks the `Charge`/
  `Order` `PAID` and issues every `RESERVED` ticket for the order (QR PNG +
  HMAC signature); on `FAILED` it marks them `FAILED`/`VOID`, releasing the
  reservation. Right after `MarkIssued`, it also inserts a
  `ticket_notifications` row per issued ticket — **inside the same
  events-DB transaction as `MarkIssued`**, since (unlike the old
  cross-database outbox design) this table lives in the `events` database
  too, so it's genuinely atomic: a ticket can never end up issued without a
  notification row, or vice versa. Once that transaction commits, it emails
  the ticket synchronously via the configured `domain.EmailSender` (no
  RabbitMQ hop) and records the outcome on that row
  (`email_sent_timestamp`/`email_sent_error`); a send failure there is
  best-effort/log-only — `notification-retry-cron` (see below) picks up
  anything still unsent. Both consumer workers scale on their own shard's
  queue depth via KEDA (min 0).
- **`tickets-api`** (Phase 8) — a synchronous REST service, the mirror
  image of `ingestion-api`: reads/writes the `events` database directly and
  never touches RabbitMQ or the outbox tables. Serves `GET
  /api/v1/orders/{id}` (order status + checkout URL, polled by the client
  after `ingestion-api`'s `201` — see the "no observable PENDING window"
  note below), `POST /api/v1/checkin` (staff-authenticated — Clerk-verified
  Bearer token, or the `fake` provider locally/in tests — ticket check-in:
  verifies the QR's HMAC signature against the stored row, flips
  `VALID`→`CHECKED_IN`), and `PATCH /api/v1/tickets/{id}/holder` (buyer-name
  correction, rate-limited per source IP via the same leaky-bucket
  `ratelimit` package `ingestion-api` uses, **no auth**).
- **`notification-retry-cron`** — a **Kubernetes `CronJob`**, not a
  `-consumer-worker` (it never consumes from RabbitMQ, so that naming
  convention doesn't apply to it): wakes up on a schedule (every 2 minutes
  by default), finds every `ticket_notifications` row still missing
  `email_sent_timestamp` whose backoff window (`next_retry_at`) has passed,
  retries sending each via the same `domain.EmailSender` (`fake` default, or
  `smtp` — a real, stdlib-only `net/smtp` sender, no third-party email SDK),
  and exits. No RabbitMQ connection, no long-running process, no metrics
  server — a job this small and short-lived doesn't need one.

**No observable "PENDING, no charge yet" window**: `checkout.ProcessOrder`
creates the `Order` row and the `Charge`/checkout URL in one transaction —
there's no point where an `orders` row exists without its `Charge` also
existing. So right after `ingestion-api`'s `201`, before
`order-consumer-worker` has processed the `order_outbox` message at all,
`GET /api/v1/orders/{id}` legitimately `404`s (no `orders` row yet) rather
than returning `200` with an empty `checkoutUrl` — a real client's polling
loop must treat an early `404` on its own just-created `orderId` as "keep
polling," not a hard failure.

**Two databases, one Postgres instance:** `ingestion-api` + `outbox-worker`
use the `outbox` database (`order_outbox` + `payment_event_outbox`);
`order-consumer-worker` + `fulfillment-consumer-worker` + `tickets-api` +
`notification-retry-cron` use the `events` database (`locations`, `events`,
`producers`, `event_areas`, `orders`, `tickets`, `charges`, `staff_users`,
`ticket_notifications`). No transaction spans the two databases — this is a
hard Postgres limitation, not just a convention — so the transactional-outbox
guarantee (atomic outbox insert) is preserved for `ingestion-api`'s own
writes; ticket notifications avoid the cross-database problem entirely by
living in the `events` database alongside the `tickets` they track, so
`fulfillment-consumer-worker` inserts the notification row in the same
transaction as `MarkIssued` — no best-effort write needed there anymore.
Each process keeps a single `DATABASE_URL` pointed at its own database.
`notification-retry-cron` still requires a `RABBITMQ_URL` env (unused —
it never connects to RabbitMQ) purely to satisfy `config.Config`'s shared
`required:"true"` tag — same "provide but ignore" precedent `ingestion-api`
already sets for its own unused `RABBITMQ_URL`.

**Payment is an outbound port, not the domain.** `internal/domain.PaymentGateway`
is the only place the system touches payment: `order-consumer-worker` calls
`CreateCheckout` to charge an order through a real gateway, and the
gateway's own webhook (verified via `VerifyWebhook` in `ingestion-api`)
confirms or fails it. Adapters: `stripe` (real, `stripe-go` v82), `fake`
(default sandbox — no network, used locally/in tests/k6), `abacatepay`/
`lemonsqueezy`/`pagarme`/`mercadopago`/`pagseguro`/`sumup` (stubs — all
Brazilian gateways except LemonSqueezy/SumUp; each stub package's doc
comment documents the provider's real request/response/webhook JSON
shapes, with doc links, researched but not built — no SDK deps, just
`domain`-only Go structs). `event_type`/`event_subtype` round-trip through the
gateway's own metadata (e.g. a Stripe Checkout Session's metadata) so that
`ingestion-api` never has to read the `events` database to route a webhook.

**Staff auth is a second, separate outbound port** (Phase 8):
`internal/domain.StaffAuthenticator` verifies a Bearer token
(`VerifyToken(token) (clerkUserID string, err error)`) — `clerk` (real,
`clerk-sdk-go/v2`, JWT verified against Clerk's JWKS endpoint) or `fake` (a
single fixed test token, default locally/in tests, no Clerk account
needed). It only answers "is this token valid" — `domain.StaffUserRepository`
(the `staff_users` table) answers "is this authenticated identity a
registered staff member, and which venue (`location_id`, nullable = any
venue) is it scoped to." Both checks happen in
`internal/adapter/http/staffauth`'s Gin middleware, wired onto **only**
`POST /api/v1/checkin` — `PATCH /api/v1/tickets/{id}/holder` stays
auth-less/rate-limit-only by explicit design choice.

Phases 1–5's plan docs (`.claude/plan.md`, `plan-phase2.md` through
`plan-phase5.md` — core system · OTel/Swagger/TestContainers/K8s+KEDA ·
per-method queues/card payments/CI/CD/Pulumi/k6 · rate limiter/canary/Grafana/
TimescaleDB · versioned migrations/retry backoff/DLQ replay/LISTEN-NOTIFY/
connection pooling/Loki-Tempo/alerting/External Secrets/PCI-DR) are **no
longer in the repo** — removed along with the payments domain they described;
git history is the only remaining record of them.
Phase 6 plan — **superseded, absorbed into Phase 7** (its ticket-relay/QR ideas live on inside the pivot below): [`.claude/plan-phase6.md`](.claude/plan-phase6.md)
Phase 7 plan — pivot from the payments domain to this Event Ticket System (two outboxes, `event_type`/`event_subtype` routing, `PaymentGateway` port, Pulumi removed in favor of Helm+KIND): [`.claude/plan-phase7-tickets-pivot.md`](.claude/plan-phase7-tickets-pivot.md)
Phase 8 plan — order-status polling, `tickets-api`, staff-authenticated check-in (Clerk), ticket-holder name correction, `govulncheck` wired into CI for real: [`.claude/plan-phase8-tickets-checkin-notifications.md`](.claude/plan-phase8-tickets-checkin-notifications.md). **Its notification-delivery design (RabbitMQ + `ticket_notification_outbox` + `notification-consumer-worker`) has since been superseded** — see the simplification below: ticket email now sends synchronously from `fulfillment-consumer-worker`, tracked in the `events`-DB `ticket_notifications` table, retried by the `notification-retry-cron` Kubernetes CronJob (no RabbitMQ involved).
Phase 9 plan — self-hosted Backstage (Software Catalog + API docs + TechDocs) cataloging all six services + `outbox-admin`, closing a real swaggo gap where `tickets-api` was never wired into `make swag`: [`.claude/plan-phase9-backstage.md`](.claude/plan-phase9-backstage.md)
Phase 10 plan — **not yet implemented, sequenced after Phase 9**. RabbitMQ → Kafka (KRaft) migration: `segmentio/kafka-go`, Bitnami Kafka on KIND, KEDA scaling switches from queue-depth to consumer-group lag: [`.claude/plan-phase10-kafka-migration.md`](.claude/plan-phase10-kafka-migration.md)
User-facing docs: [`README.md`](README.md)

---

## Architecture: Clean Architecture

**Dependency rule:** outer layers depend on inner layers, **never the reverse**.

```
infrastructure  (Gin · GORM · amqp091 · Postgres · config · main / DI)
  └── adapter   (http handlers/DTOs · GORM repos · RabbitMQ pub/consumer · PaymentGateway/EmailSender/StaffAuthenticator adapters · QR)
        └── usecase   (PlaceOrder · ReceivePaymentEvent · DispatchOutbox · ProcessOrder · IssueTickets · CheckIn · UpdateHolder · SendTicketNotification)
              └── domain   (entities + port interfaces) ← ZERO external imports
```

**Critical convention:** `internal/domain/` must **never** import Gin, GORM,
`amqp091-go`, `stripe-go`, or any other third-party framework. Violations
break the dependency rule. Frameworks are wired in at `cmd/*/main.go` via
domain port interfaces.

---

## Where things live

| What | Path |
|---|---|
| Entities (`Order`, `Ticket`, `Charge`, `Event`, `Location`, `OutboxMessage`, `TicketNotification`, `StaffUser`) | `internal/domain/` |
| Port interfaces (`OrderRepository`, `TicketRepository`, `ChargeRepository`, `EventRepository`, `LocationRepository`, `OutboxRepository`, `TicketNotificationRepository`, `PaymentGateway`, `EmailSender`, `StaffAuthenticator`, `StaffUserRepository`, `TicketQR`, `Publisher`, `UnitOfWork`) | `internal/domain/` |
| `PlaceOrder` use-case (`POST /api/v1/orders` → `order_outbox`) | `internal/usecase/order/` |
| `ReceivePaymentEvent` use-case (`POST /api/v1/webhooks/payments/{provider}` → `payment_event_outbox`) | `internal/usecase/webhook/` |
| `DispatchOutbox` use-case — Transactional Outbox core (poll → dedup → publish → mark), generalized over a table name so one implementation drives both outboxes | `internal/usecase/outbox/` |
| `ProcessOrder` use-case (order-consumer-worker: upsert Location/Event, reserve Tickets, `PaymentGateway.CreateCheckout`, persist Charge) | `internal/usecase/checkout/` |
| `IssueTickets` use-case (fulfillment-consumer-worker: mark Charge/Order PAID + issue Tickets (or FAILED/VOID), insert a `ticket_notifications` row atomically with `MarkIssued`, then email each ticket synchronously) | `internal/usecase/fulfillment/` |
| `CheckIn` use-case (tickets-api: verify QR signature + venue scope, flip VALID→CHECKED_IN) | `internal/usecase/checkin/` |
| `UpdateHolder` use-case (tickets-api: correct a ticket's buyer name) | `internal/usecase/ticketholder/` |
| `SendTicketNotification` use-case (`Send` is duplicated into `usecase/fulfillment` too — use-cases must not depend on one another — but `RetryPending` is notification-retry-cron's whole job: find every `ticket_notifications` row still unsent and retry) | `internal/usecase/notification/` |
| Gin router(s), order/webhook/order-status/checkin/ticketholder handlers, DTOs, middleware | `internal/adapter/http/` |
| Staff-auth Gin middleware (Bearer token → `StaffUser` on context, check-in route only) | `internal/adapter/http/staffauth/` |
| GORM DB models + repository implementations + UnitOfWork | `internal/adapter/persistence/` |
| RabbitMQ publisher + one generalized consumer (`MessageProcessor` interface, shared by order-consumer-worker and fulfillment-consumer-worker) | `internal/adapter/messaging/` |
| `PaymentGateway` adapters (`stripe` real, `fake` sandbox, `abacatepay`/`lemonsqueezy`/`pagarme`/`mercadopago`/`pagseguro`/`sumup` stubs) | `internal/adapter/paymentgateway/` |
| `EmailSender` adapters (`fake` default, `smtp` real via stdlib `net/smtp`) + the shared `EMAIL_PROVIDER` selection factory | `internal/adapter/emailsender/` |
| `StaffAuthenticator` adapters (`clerk` real via `clerk-sdk-go/v2`, `fake` default) | `internal/adapter/staffauth/` |
| Ticket QR PNG + HMAC signing/verification | `internal/adapter/ticketqr/` |
| `Config` struct (envconfig) | `internal/infrastructure/config/` |
| DB connection bootstrap | `internal/infrastructure/database/` |
| RabbitMQ connection + `EventTypes` registry + topology declaration | `internal/infrastructure/rabbitmq/` |
| Composition root / DI (ingestion-api — HTTP + rate limit, writes both outboxes) | `cmd/ingestion-api/main.go` |
| Composition root / DI (outbox-worker — two `DispatchOutbox` loops + LISTEN/NOTIFY) | `cmd/outbox-worker/main.go` |
| Composition root / DI (order-consumer-worker) | `cmd/order-consumer-worker/main.go` |
| Composition root / DI (fulfillment-consumer-worker — also wires `EmailSender`) | `cmd/fulfillment-consumer-worker/main.go` |
| Composition root / DI (tickets-api — HTTP, events DB, no RabbitMQ) | `cmd/tickets-api/main.go` |
| Composition root / DI (notification-retry-cron — a Kubernetes CronJob, no RabbitMQ, runs `RetryPending` once and exits) | `cmd/notification-retry-cron/main.go` |
| Maintenance CLI (DLQ replay/drain, `--outbox`/`--stream`/`--event-type`/`--event-subtype` flags) | `cmd/outbox-admin/main.go` |
| Versioned migrations, split per database | `migrations/outbox/`, `migrations/events/` |
| Docker Compose (local dev) | `docker-compose.yml` |
| Multi-stage Dockerfile (ARG SERVICE) | `Dockerfile` |
| Helm chart (fixed-replica `ingestion-api`/`tickets-api`; one `outbox-worker` Deployment + KEDA postgresql ScaledObject summing both outboxes; one order/fulfillment-consumer-worker Deployment/Rollout + ScaledObject pair per `eventShards` entry; one `notification-retry-cron` Kubernetes CronJob, no Deployment/ScaledObject; `canary.enabled` switches Deployment+HPA ↔ Argo Rollout+AnalysisTemplate for the four Phase 7 services — `tickets-api`/`notification-retry-cron` have no Rollout variant, the latter because a CronJob isn't a canary-able rollout target) | `helmcharts/transaction-outbox/` |
| KIND cluster + values override (the actual deploy/test path — see "What NOT to do") | `infra/kind/` |
| Rate limiter (leaky-bucket IP throttle; ingestion-api's `POST /orders`, tickets-api's `PATCH .../holder`) | `internal/adapter/http/ratelimit/` |
| Prometheus/Grafana provisioning (dashboards, datasource, postgres-exporter queries) | `observability/` |
| GitHub Actions CI/CD (one workflow per microservice — see below) | `.github/workflows/` |
| Backstage developer portal (Software Catalog + API docs + TechDocs, Phase 9 — Node/TypeScript, the repo's only non-Go toolchain, **not** built/linted via `make build`/`make lint`) | `backstage/` |
| Backstage catalog entities (System + Group + one Component per binary + API entities for ingestion-api/tickets-api) | `catalog-info.yaml` |
| TechDocs source (rendered by Backstage) | `mkdocs.yml`, `docs/index.md` |
| PII redaction (`Redact`/`RedactJSON`, masks `email`/`document`/`validationCode`/`signature` — buyer **name** deliberately NOT masked, see Phase 8 plan Part I) | `internal/domain/pii/` |
| Integration tests (TestContainers: Postgres + RabbitMQ) | `tests/integration/` |
| k6 load tests (order intake, KEDA autoscaling, order-consumer-worker in isolation) | `loadtest/` |

---

## Key conventions

- **GORM structs** live only in `adapter/persistence`, not in `domain`. Domain
  entities are plain Go structs. Repositories map between them. The two
  outbox tables (`order_outbox`/`payment_event_outbox`) share ONE GORM
  repository implementation (`GORMOutboxRepository`), parameterized by table
  name via `.Table(name)` rather than a fixed `TableName()` model — the
  tables are schema-identical. `ticket_notifications` is a different shape
  (no RabbitMQ routing columns needed) and gets its own repository,
  `GORMTicketNotificationRepository`.
- **Port interfaces** are defined in `domain` and implemented in `adapter`.
  `usecase` depends on the interface type, never on the concrete adapter.
- **UnitOfWork** (`domain/uow.go`) abstracts DB transactions so `usecase` can
  compose multiple repo operations atomically without importing GORM.
- **Upsert-then-return-id gotcha**: `LocationRepository.UpsertBySourceVenueID`/
  `EventRepository.UpsertBySourceEventID` rely on Postgres `INSERT ... ON
  CONFLICT DO UPDATE ... RETURNING id`, and **must** pass an explicit
  `clause.Returning{Columns: []clause.Column{{Name: "id"}}}` — GORM only
  auto-adds `RETURNING` for a primary key left at its zero value, and both
  callers always supply a fresh client-generated candidate UUID, so without
  the explicit clause a conflict would silently return the phantom
  (never-inserted) candidate id instead of the real existing row's id. This
  was a real bug caught by the integration suite's 50-concurrent-orders test
  (all sharing one venue) — see `.claude/plan-phase7-tickets-pivot.md`'s
  progress notes for the war story.
- **Two idempotency-key formulas**, both `sha256`-derived from the caller's
  own event identity, never a server-generated UUID:
  - Orders: `sha256("order:" + sourceOrderId [+ ":" + Idempotency-Key-header])`
    (`usecase/order`) — a redelivered order carries the same `sourceOrderId`.
  - Payment-gateway webhooks: `sha256(provider + ":" + rawEventId)`
    (`usecase/webhook`) — the gateway's own event id is the dedup boundary,
    not `ProviderRef` (one charge can, in principle, emit more than one
    event over its lifetime).
- **Order wire format**: `{sourceOrderId, eventType, eventSubtype, eventId,
  eventName?, venue{id,name,city}, tickets[{id,section,row,seat,price,currency}],
  customer{name,email,document}}`. `eventType`/`eventSubtype` must be a
  registered pair (`internal/infrastructure/rabbitmq.EventTypes`) or the
  request is rejected `400` — an unregistered pair has no bound RabbitMQ
  queue, so publishing it would be a topic-exchange black hole (matched by
  no binding, silently dropped). All tickets in one order must share one
  `currency` — mixed-currency orders aren't supported.
- **Amount conversion at the boundary:** the wire format's `tickets[].price`
  is a decimal float (currency units, e.g. `150.00`).
  `internal/adapter/http/order_handler.go` converts it to `int64` minor units
  (`round(price * 100)`) immediately — domain and persistence code never see
  a float.
- **`(event_type, event_subtype)` replaces payment methods as the routing
  key everywhere** — RabbitMQ queue/DLQ/retry-queue names, the topic-exchange
  routing key, and (denormalized) columns on `order_outbox`/
  `payment_event_outbox`/`orders`/`tickets`. The registry
  (`internal/infrastructure/rabbitmq.EventTypes`) is a `map[string][]string`
  (e.g. `"CONCERT": ["ROCK","POP",...]`). Adding a new pair is a one-line
  registry change (+ a values.yaml/`eventShards`/`docker-compose.yml` entry
  to actually run a consumer for it) — no code path needs to change, same as
  the old per-payment-method design it replaced.
- **`Stream` (`internal/infrastructure/rabbitmq.OrderStream`/`PaymentEventStream`)**
  picks the queue-name/routing-key prefix (`events.*`/`order.*` vs.
  `payments.*`/`payment.*`) a message routes under; `AMQPPublisher` derives it
  from `OutboxMessage.AggregateType` (`"order"` or `"payment_event"`).
- **Outbox status state machine (unchanged, shared by both tables):** `NEW` →
  `PUBLISHED`, `NEW` → `RETRYING`, `RETRYING` → `PUBLISHED`, `RETRYING` →
  `DEAD_LETTER` (after max retries).
- **`DispatchOutbox`** (`usecase/outbox`) is the Transactional Outbox core: it
  runs as **two goroutines inside the `outbox-worker` process**
  (`cmd/outbox-worker/main.go`), one per outbox table, sharing one publisher
  and connection. Use `context.Context` for graceful shutdown. Use
  `FOR UPDATE SKIP LOCKED` so multiple replicas (KEDA can scale past 1 under
  backlog) never double-publish. Only `order_outbox` gets a LISTEN/NOTIFY
  trigger (channel `order_outbox_new`) — `payment_event_outbox` is poll-only
  (lower volume, no low-latency need).
- **Publisher confirms** must be enabled on the `DispatchOutbox` AMQP channel. Never
  mark a row `PUBLISHED` before the confirm ACK arrives.
- **Manual ack + prefetch** on every consumer worker — one generalized
  `AMQPConsumer` (`internal/adapter/messaging/consumer.go`) parameterized by a
  `MessageProcessor` interface serves `order-consumer-worker`
  (`checkout.ProcessOrder`) and `fulfillment-consumer-worker`
  (`fulfillment.IssueTickets`); only call `msg.Ack()` after the DB
  transaction commits successfully.
- **Ticket issuance**: `fulfillment.IssueTickets` generates a random
  `validationCode`, an HMAC-SHA256 `signature` over `ticketID + ":" +
  validationCode` (key: `TICKET_SIGNING_SECRET`), a compact `qrContent` token
  embedding both, and renders it as a PNG (`internal/adapter/ticketqr`, via
  `github.com/skip2/go-qrcode`) — never persisted/published until the Charge
  is confirmed `PAID`. `ticketqr.Verify(ticketID, validationCode, signature,
  secret) bool` is the symmetric check `usecase/checkin.CheckIn` calls,
  recomputing the HMAC from the ticket's **stored** row (never trusting the
  check-in request's fields in isolation, so a QR copied from a
  different/voided ticket can't be replayed). Right after `MarkIssued`,
  `IssueTickets` inserts the ticket's `ticket_notifications` row in the same
  transaction, then (once committed) emails it synchronously and records
  `email_sent_timestamp`/`email_sent_error` — `notification-retry-cron`
  retries anything still unsent later.

---

## Module

```
github.com/alonsomachado/transaction-outbox-go
```

Go version: **1.26** (`go 1.26`, toolchain `go1.26.4`)

---

## Build / run commands

Go is **not installed on the host machine** — all Go tooling runs inside Podman
containers via `make` targets. Never run `go` directly on the host.

```bash
# Build, test, lint — all run inside golang:1.26-alpine via Podman
make build    # go build ./...
make test     # go test -race ./...
make tidy     # go mod tidy
make lint     # golangci-lint run ./...  (golangci/golangci-lint:latest image)

# Podman Compose — starts Postgres + RabbitMQ + the app services
# (ingestion-api, outbox-worker, and order/fulfillment-consumer-worker for
# two shards by default: CONCERT/ROCK, SPORTS/FOOTBALL)
make up       # podman compose up --build -d
make logs     # tail logs from all services
make down     # podman compose down -v (removes volumes)
make seed     # curl a sample order POST to the ingestion-api

# Backstage (backstage/) is Node/TypeScript, not Go — it is NOT covered by
# make build/test/lint above. It has its own containerized targets and its
# own CI workflow (backstage.yml); see "Where things live".
make backstage-build
make backstage-up
```

## Linting rules

**Always run `make lint` after any code change.** The linter (`golangci-lint`
running inside Podman) must report zero issues before a change is considered
done. Key rules enforced:

- **`errcheck`** — every error return must be checked. For `Close()` calls
  inside `defer` where the error is unactionable, use `defer func() { _ = x.Close() }()`.
  For `Close()` calls in `main` before the server starts, log the error.
- All default golangci-lint linters apply — do not add `.golangci.yml` overrides
  to silence findings; fix the code instead.

---

## CI/CD (GitHub Actions)

**One workflow per microservice, not one shared pipeline** — six now:
[`.github/workflows/ingestion-api.yml`](.github/workflows/ingestion-api.yml),
[`outbox-worker.yml`](.github/workflows/outbox-worker.yml),
[`order-consumer-worker.yml`](.github/workflows/order-consumer-worker.yml),
[`fulfillment-consumer-worker.yml`](.github/workflows/fulfillment-consumer-worker.yml),
[`tickets-api.yml`](.github/workflows/tickets-api.yml), and
[`notification-retry-cron.yml`](.github/workflows/notification-retry-cron.yml).
Each is triggered only by changes to its own `cmd/<service>/**` path (plus
shared `internal/**`/`go.mod`/`go.sum`/`Dockerfile`), so a change scoped to
one service never triggers or gates the others. All six follow the same
gate order:

```
build → lint (golangci-lint + actionlint + helm lint + govulncheck, GATE) → unit-tests (GATE) → upload (ECR)
                                                                                     └── integration-tests (optional, flag-gated, never blocks)
```

`lint` runs **four** checks: `golangci-lint` over the Go code, `actionlint`
over the workflow YAML itself (catches a broken pipeline the same way
golangci-lint catches broken Go), `helm lint` over
`helmcharts/transaction-outbox` (catches a broken K8s manifest/values schema
before it's ever installed), and `govulncheck` (`go run
golang.org/x/vuln/cmd/govulncheck@latest ./...` — checks `go.sum`'s
dependency tree against the official Go vulnerability database for CVEs
**actually reachable** from this code's call graph, wired into all six
workflows as of Phase 8; it caught a real reachable CVE in `go-jose/v3`
pulled in transitively by the new Clerk SDK dependency, fixed by bumping
that dependency directly). `integration-tests` (the
TestContainers suite) is a safety measure only — off by default, triggered
via `workflow_dispatch` or a `ci:integration` PR label, and never wired into
anything `upload` depends on. There is no automated `deploy` job — Pulumi was
removed from the project; Helm + KIND is the deploy/test path, applied
manually. See [`.github/workflows/README.md`](.github/workflows/README.md)
for the full rationale.

---

## What NOT to do

- Do **not** import framework packages (`gin`, `gorm.io`, `amqp091`,
  `stripe-go`) in `internal/domain/` or `internal/usecase/`.
- Do **not** put GORM struct tags on domain entities.
- Do **not** ACK a RabbitMQ message before the DB transaction commits.
- Do **not** mark an outbox row `PUBLISHED` before receiving a publisher confirm.
- Do **not** fold `DispatchOutbox` back into `ingestion-api`. It is its own
  binary, `outbox-worker` (`cmd/outbox-worker/main.go`), so it scales on
  outbox backlog independently and ingestion-api stays broker-independent.
  There are **six** service binaries plus the `outbox-admin` CLI.
- Do **not** point a service at the wrong database. ingestion-api and
  outbox-worker → `outbox` DB; order-consumer-worker,
  fulfillment-consumer-worker, tickets-api, and notification-retry-cron →
  `events` DB. New outbox-related migrations go in `migrations/outbox/`,
  events-domain ones (including `ticket_notifications`) in
  `migrations/events/`.
- Do **not** try to route `usecase/fulfillment.IssueTickets`'s
  `notificationRepo.Create` call through a **fresh, non-tx context** (`nil`
  `uow`) the way the old cross-database `ticket_notification_outbox` enqueue
  had to — `ticket_notifications` lives in the same `events` database as
  `tickets` now, so it belongs **inside** `confirmAndIssue`'s transaction
  (same `uow`), same as `MarkIssued`. That's what makes it atomic; passing a
  non-tx context here would silently reintroduce the old best-effort gap for
  no reason.
- Do **not** use `AutoMigrate` in tests — use a test transaction rollback or a
  dedicated test schema.
- **Any service that consumes from RabbitMQ must have a name ending in
  `-consumer-worker`** (company convention) — e.g.
  `order-consumer-worker`/`fulfillment-consumer-worker`, never
  `order-worker`/`fulfillment-worker`. `outbox-worker` is exempt (it only
  publishes, never consumes); `notification-retry-cron` is exempt too (it's
  a Kubernetes CronJob that never touches RabbitMQ at all).
- Do **not** reintroduce card/PAN handling anywhere in this codebase. Payment
  is Stripe-hosted checkout (`PaymentGateway.CreateCheckout` redirects to a
  gateway-hosted page) — no card data ever reaches this system, which is
  also why there's no PCI-DSS card-data scope anymore (see `SECURITY.md`).
- Do **not** run `git commit` or `git push` in this repo, ever, even if asked
  in a way that sounds like a general go-ahead (e.g. "vou comitar e subir
  tudo"). The user commits and pushes everything themselves. Stage/diff/log
  read-only git commands are fine; leave the working tree's changes
  uncommitted and tell the user what's ready for them to commit.
- Pulumi has been **removed from this project** (`infra/pulumi/` no longer
  exists) — Helm + KIND is the deploy/test path now (`infra/kind/`, `make
  k8s-apply`). It may come back as its own initiative later; don't
  reintroduce it without being asked. `grep`/`find`, and read-only `podman
  run`/`podman logs`/`podman ps` are always fine to run.
