# Plan — Phase 7: Domain pivot to a Tickets-for-Events platform

> **Phase 7 — fully implemented (as of 2026-07-06; coverage rule added
> 2026-07-07, Part G).** This is the plan that was executed to pivot the
> system; kept as the historical record of the decisions made and the order
> they shipped in, not a forward-looking TODO list anymore. It is a **domain
> pivot, not an increment**: the payments
> webhook domain is removed and the system is rebuilt around **events & ticket
> orders**, with payment demoted to an **outbound port/adapter** to a real
> gateway. Four design decisions already made with the user:
> 1. **Routing key = event category & genre.** RabbitMQ queues and the domain
>    DB are sharded by `event_type` × `event_subtype` (e.g. `CONCERT`/`ROCK`),
>    a direct replacement of today's per-payment-method queues.
> 2. **Payment = outbound charge + inbound webhook.** A consumer calls the
>    gateway to create a checkout/charge; a webhook endpoint confirms
>    success/failure, which triggers ticket issuance. Full port-adapter, both
>    directions.
> 3. **Gateway scope = Stripe real, others stubbed.** Build the `PaymentGateway`
>    port + a production Stripe adapter + a `fake` sandbox adapter (local/tests/
>    k6); `abacatepay`/`lemonsqueezy` are thin stubs to fill in later.
> 4. **Replace payments with the events/tickets domain.** The `payments` DB and
>    hypertables and the payment webhook path are dropped. Core DBs become
>    `outbox` + a new `events` DB. This **supersedes and absorbs the Phase 6
>    plan** (`ticket_outbox` relay + `tickets` DB) — Phase 6 is folded into this
>    pivot and does not ship separately.

## Context

The current system is a **Transactional Outbox for a payments domain**:
`ingestion-api` accepts payment-provider webhooks (`POST /api/v1/payments`),
lands them in `outbox_messages`, `outbox-worker` relays to **per-method**
RabbitMQ queues (`rmq.Methods`), and `consumer-worker` writes the `payments`
DB. A parallel, half-built ticket path (`POST /api/v1/ticket` → `ticket_outbox`,
no relay) exists from Phase 6 groundwork.

Phase 7 keeps the **architecture** (Clean Architecture, Transactional Outbox,
per-shard queues + KEDA, two logical DBs in one Postgres instance, publisher
confirms, manual-ack consumers, DLQ + backoff) but swaps the **domain**:

- **Producers** run **Events** (`event_type` × `event_subtype`) at **Locations**.
- Customers place **Orders** for **Tickets** to an event.
- An order is charged through an external **payment gateway** (Stripe now;
  AbacatePay/LemonSqueezy later) via an outbound port; the gateway's **webhook**
  confirms payment, which **issues** the tickets (signed QR PNGs).

The transactional-outbox guarantee is unchanged: `ingestion-api` still writes
**only** outbox tables (order intake + webhook intake), never the domain
tables, so no transaction spans the two DBs. The webhook path lands the
gateway callback durably in a second outbox so ingestion-api stays
broker-independent and returns a fast `200` to the gateway.

### Target pipeline

```
POST /api/v1/orders ─► ingestion-api ─► order_outbox (NEW)            [outbox DB]
                                              │
                       outbox-worker (poll + LISTEN/NOTIFY)
                       route key: order.<event_type>.<event_subtype>
                                              ▼
                    events.<type>.<subtype>.queue  ──► order-consumer-worker    [events DB]
                       • upsert Producer/Location/Event
                       • reserve Tickets (PENDING_PAYMENT)
                       • PaymentGateway.CreateCheckout()  ──► Stripe (outbound)
                       • persist Charge (PENDING) + checkout URL
                                              ┆
                    customer pays at gateway … gateway calls back
                                              ▼
POST /api/v1/webhooks/payments/{provider} ─► ingestion-api
                       • VerifyWebhook(sig)   payment_event_outbox (NEW) [outbox DB]
                                              │
                       outbox-worker  route key: payment.<type>.<subtype>
                                              ▼
                    payments.<type>.<subtype>.queue ──► fulfillment-consumer-worker
                       • confirmed ► Charge=PAID, issue Tickets (QR PNG + HMAC)
                       • failed    ► Charge=FAILED, release reservation
```

Four service binaries (+ the `outbox-admin` CLI): `ingestion-api`,
`outbox-worker`, `order-consumer-worker` (renamed `consumer-worker`), `fulfillment-consumer-worker`
(new).

---

## What is REMOVED (payments domain)

- `internal/domain/payment.go`, the payment wire format / `schema.go`, the
  polymorphic `MethodDetails`, `ValidateMethod` + `card.go`/`maskPAN`,
  `rmq.Methods` and all `*Method*` helpers, `internal/usecase/ingest` (payment),
  `internal/usecase/consume` (`ProcessMessage`), `payment_repo.go`,
  `POST /api/v1/payments`, `migrations/payments/*` (TimescaleDB hypertables),
  the whole `payments` database.
- **Kept / repurposed:** the outbox core (`usecase/outbox` → generalized),
  `domain/backoff.go`, `domain/uow.go`, `domain/uuid.go`, `database.Listener`
  (LISTEN/NOTIFY), `telemetry`, `logging`, `observability`, `ratelimit`, the
  RabbitMQ topology *shape* (exchange + per-shard quorum queue + DLQ + retry-TTL
  queue), and `internal/domain/pii` — now masking **customer** PII (email,
  document) and the last-4 card data that arrives inside `payment_details`.

---

## Part A — Domain & DB rebuild (`events` DB, sharded by type/subtype)

### A1. Domain entities & ports (`internal/domain/`)
Plain Go structs, **zero framework imports** (dependency rule):
- `producer.go` — `Producer{ID, Name, LogoURL, WebsiteURL}`.
- `location.go` — `Location{ID, Name, Address…, Lat/Lng, SourceVenueID}`.
- `event.go` — `Event{ID, EventType, EventSubtype, Name, DateUTC, LocationID,
  ProducerID, SourceEventID}` + `EventArea` (auxiliary child). Add an
  `EventType`/`EventSubtype` value pair + a canonical registry (see A4).
- `order.go` — `Order{ID, SourceOrderID, EventType, EventSubtype, Customer,
  Items[]OrderItem, Amount int64 minor units, Currency, Status}` (`PENDING` →
  `RESERVED` → `PAID`/`FAILED`).
- `ticket.go` (repurpose) — `Ticket{ID(v7), OrderID, EventID, SourceTicketID,
  Section/Row/Seat, Price int64, Currency, BuyerID/Name/Email, QRPNG []byte,
  QRContent, ValidationCode, Signature, Status}` (`RESERVED`/`VALID`/`VOID`).
- `charge.go` — `Charge{ID, OrderID, Provider, ProviderRef, Amount, Currency,
  Status}` (`PENDING`/`PAID`/`FAILED`) — the gateway-transaction ledger.
- **Ports:** `ProducerRepository`, `LocationRepository`, `EventRepository`,
  `OrderRepository`, `TicketRepository`, `ChargeRepository`, plus `TicketQR`
  (QR/validation service) and `PaymentGateway` (Part C). Repos accept a
  `domain.UnitOfWork` (reuse `uow.go`).

### A2. `events` database plumbing (mirror the payments two-DB split)
- New migration set `migrations/events/000001_init.up.sql` (+ down): the six
  tables above. Replace `migrations/payments/`.
- `observability/postgres/init-payments.sql` → `init-events.sql` (createdb
  `events`, drop `payments`); update the KIND
  [migrate-job.yaml](infra/kind/migrate-job.yaml) and the compose `migrate-*`
  one-shots.
- [Makefile](Makefile): `MIGRATE_DSN_EVENTS` replaces `MIGRATE_DSN_PAYMENTS`.
- PgBouncer wildcard `[databases]` already pools the new DB; each process keeps
  a single `DATABASE_URL` (events DB for order-consumer-worker + fulfillment-consumer-worker).

### A3. Partition the domain **by `event_type` × `event_subtype`** — superseded by the index-only fallback below (see outcome note)

**Outcome (2026-07): the index-only fallback was implemented, not
declarative partitioning.** `migrations/events/000001_init.up.sql` ships
plain `orders`/`tickets` tables with a `(event_type, event_subtype)` btree
index, not `PARTITION BY LIST`. Rationale: this is a demo-scale system with
no per-genre vacuum/retention pressure yet to justify the operational cost
of a `CREATE … PARTITION` migration per new `(type, subtype)` pair; the
index gets the same query shape (any lookup/scan filtered on the pair still
hits one index) with zero migration ceremony when the registry grows. This
is documented at the top of the migration file itself and in `CLAUDE.md`.
Revisit partitioning if per-genre vacuum/retention independence becomes a
real operational need — the section below is kept as the design that would
require reversing this choice, not a currently-open TODO.

Per the routing decision, the hot tables were considered for **Postgres
declaratively list-partitioning**:
- `orders` and `tickets` `PARTITION BY LIST (event_type)`, each type
  sub-partitioned `BY LIST (event_subtype)` (or a composite `event_type ||
  '/' || event_subtype` list partition if we prefer one level). `events` may
  stay unpartitioned (low cardinality) with a `(event_type, event_subtype)`
  index.
- The migration creates one partition per `(type, subtype)` in the canonical
  registry (A4). **Trade-off, called out:** adding a new `(type, subtype)`
  then requires (a) a `rmq` registry entry, (b) a `CREATE … PARTITION`
  migration. Fallback if we want zero-migration extensibility: **no
  partitioning**, just a `(event_type, event_subtype)` btree index — same query
  shape, less operational ceremony. **Recommend partitioning** to make "the
  database too is sharded by type/subtype" literally true and to keep per-genre
  vacuum/retention independent.

### A4. Canonical `(event_type, event_subtype)` registry (replaces `rmq.Methods`)
In `internal/infrastructure/rabbitmq/rabbitmq.go`, replace `Methods` with an
`EventTypes` registry — a `map[string][]string` (type → subtypes) or
`[]EventKind{Type, Subtype}`. Seed, e.g.:
`CONCERT`→{ROCK, POP, ELECTRONIC, SAMBA}, `SPORTS`→{FOOTBALL, BASKETBALL, UFC},
`THEATER`→{PLAY, STANDUP, MUSICAL}, `CONFERENCE`→{TECH, BUSINESS}. Same
black-hole rationale as today: an order whose `(type, subtype)` has no bound
queue is rejected `400` at the HTTP boundary — adding a pair is a small,
localized change (registry + optional partition + optional validation case).

---

## Part B — RabbitMQ topology & the outbox relay (routed by type/subtype)

### B1. Topology (`internal/infrastructure/rabbitmq/rabbitmq.go`)
- `Exchange = "tickets.exchange"` (topic), `DLX = "tickets.dlx"` (direct).
- Helpers keyed by `(type, subtype)` instead of `method`:
  `QueueFor`, `DLQFor`, `RetryQueueFor`, `RoutingKeyFor`, `DLXRoutingKeyFor`,
  `QueueForKey`/reverse-lookup. Names like `events.concert.rock.queue`,
  routing key `order.concert.rock`; the payment-event stream reuses the same
  shard key with a `payment.<type>.<subtype>` routing prefix so the **whole
  system is sharded identically**.
- `DeclareTopology` loops the registry declaring, per pair: order queue,
  payment-event queue, both DLQs, both retry-TTL queues (keep the quorum +
  explicit-reject-to-DLX pattern documented in `declareMethodQueue`, incl. the
  RabbitMQ 4.3 delivery-limit caveat).

### B2. Two outboxes, one relay (`internal/usecase/outbox` → generalize)
- New `migrations/outbox/` init: `order_outbox` and `payment_event_outbox`,
  both with the full state machine (`retry_count`, `last_error`, `next_retry_at`,
  partial index on `status IN ('NEW','RETRYING')`) and a `LISTEN/NOTIFY`
  trigger. Replaces `outbox_messages` + `ticket_outbox`. Each row carries its
  `event_type`/`event_subtype` (denormalized for routing without parsing the
  payload).
- Generalize `DispatchOutbox` to take a table + routing-key function so the
  same use-case relays both outboxes; `outbox-worker` runs two dispatch loops
  (order + payment-event), both routed by `(type, subtype)`. Keep LISTEN/NOTIFY
  fast-path for orders; poll for payment events (low volume).
- `outbox_repo.go`: generalize `FetchPending`/`MarkPublished`/`MarkRetrying`/
  `MarkDeadLetter`/`CountPending` (keep `FOR UPDATE SKIP LOCKED` + batch mark).
- `AMQPPublisher` publishes on a confirm-enabled channel to
  `RoutingKeyFor(type, subtype)`; `MessageId` = idempotency key.

### B3. KEDA scaler (`helm outbox-worker/scaledobject.yaml`)
Query sums NEW/RETRYING across **both** `order_outbox` and
`payment_event_outbox`.

---

## Part C — Payment gateway port + adapters (outbound + inbound)

### C1. Port (`internal/domain/payment_gateway.go`)
```
PaymentGateway interface {
    CreateCheckout(ctx, ChargeRequest) (CheckoutSession, error)   // outbound
    VerifyWebhook(rawBody []byte, headers) (PaymentEvent, error)  // inbound
}
```
`ChargeRequest{OrderID, Amount, Currency, Customer, SuccessURL, Metadata}`;
`CheckoutSession{ProviderRef, CheckoutURL}`; `PaymentEvent{ProviderRef,
OrderID, Outcome CONFIRMED|FAILED, RawEventID}`. Domain-only, no SDK imports.

### C2. Adapters (`internal/adapter/paymentgateway/`)
- `stripe/` — **real**: `github.com/stripe/stripe-go` Checkout Sessions +
  `webhook.ConstructEvent` signature verification (uses `STRIPE_WEBHOOK_SECRET`).
- `fake/` — sandbox: deterministic `ProviderRef`, an in-process "auto-confirm"
  or a signed test webhook the k6/integration harness can POST back. Default
  provider locally and in tests.
- `abacatepay/`, `lemonsqueezy/` — **stubs** implementing the port, returning
  `ErrNotImplemented`, ready to fill in.
- Selected by config `PAYMENT_PROVIDER` (default `fake`), wired in each
  `main.go` via the port (no use-case change to swap providers).

### C3. HTTP: order intake + webhook intake (`internal/adapter/http/`)
- Replace `handler.go`/`dto.go`/`card.go` (payments) with `order_handler.go` +
  `order_dto.go`: `POST /api/v1/orders` validates `(event_type, event_subtype)`
  against the registry, converts `amount` decimal→`int64` minor units at the
  boundary (keep the existing convention), computes the idempotency key from the
  order's own identity (`source_order_id`/`event_id` + optional
  `Idempotency-Key` header — **never** the server UUID), and writes
  `order_outbox` via the `PlaceOrder` use-case. Returns `201`
  (`accepted`/`duplicate`).
- `webhook_handler.go`: `POST /api/v1/webhooks/payments/{provider}` →
  `PaymentGateway.VerifyWebhook` → `ReceivePaymentEvent` use-case writes
  `payment_event_outbox`; returns `200` fast. Dedup on the gateway's own event
  id (`UNIQUE`). This route is **excluded from the leaky-bucket rate limiter**
  and from body-mangling middleware (Stripe needs the raw body for signature
  verification).

### C4. Use-cases
- `internal/usecase/order/place_order.go` — ingestion side (writes `order_outbox`).
- `internal/usecase/webhook/receive_payment_event.go` — ingestion side (writes
  `payment_event_outbox`).
- `internal/usecase/checkout/process_order.go` — **order-consumer-worker**: upsert
  Producer/Location/Event, reserve Tickets, `CreateCheckout` (outbound), persist
  Charge. Idempotent via `orders.source_order_id UNIQUE` / `ON CONFLICT`.
- `internal/usecase/fulfillment/issue_tickets.go` — **fulfillment-consumer-worker**: on
  `CONFIRMED` mark Charge `PAID` + issue Tickets (QR PNG + HMAC signature via
  `TicketQR`); on `FAILED` mark Charge `FAILED` + release the reservation.
  Idempotent via `tickets.source_ticket_id UNIQUE`.

### C5. QR + validation (`internal/adapter/ticketqr/`)
Per Phase 6: `validation_code` (random UUID), `signature = HMAC-SHA256(ticketID
+ ":" + validation_code, TICKET_SIGNING_SECRET)`, compact `qr_content`
token/URL, and a rendered **PNG** via `github.com/skip2/go-qrcode`. Secret from
config (dev default in compose, Helm secret in cloud).

### C6. Consumers (`internal/adapter/messaging/`)
- `order_consumer.go` — binds `events.<type>.<subtype>.queue`, prefetch + manual
  ack, retry-TTL/DLQ on failure, calls `ProcessOrder`. Keep the header-based
  retry-count + explicit `Reject(requeue=false)` DLQ pattern from today's
  `consumer.go`.
- `payment_consumer.go` — binds `payments.<type>.<subtype>.queue`, calls
  `IssueTickets`. **Ack only after the DB tx commits** (unchanged rule).

### C7. Config (`internal/infrastructure/config/config.go`)
Add `PaymentProvider` (`fake`/`stripe`/…), `StripeSecretKey`,
`StripeWebhookSecret`, `AbacatePayKey`, `LemonSqueezyKey`, `TicketSigningSecret`,
`ConsumerQueue` (replaces `PaymentQueue`; the single queue a consumer binary
binds). Remove payment-specific keys. Keep one `DATABASE_URL` per process.

---

## Part D — Binaries, deploy, CI, tests, k6, docs

### D1. Binaries (`cmd/`)
- `ingestion-api` — swap payment handler → order + webhook handlers; still no
  RabbitMQ, fixed 1 replica.
- `outbox-worker` — two dispatch loops (order + payment-event), routed by
  `(type, subtype)`.
- `order-consumer-worker` — **rename** `consumer-worker`; DI for `ProcessOrder` + the
  selected `PaymentGateway`.
- `fulfillment-consumer-worker` — **new**; DI for `IssueTickets` + `TicketQR`.
- `outbox-admin` — retarget DLQ replay/prune at the two new outbox tables.

### D2. Helm (`helmcharts/transaction-outbox/`)
- Rename `consumer-worker/` templates → `order-consumer-worker/`; add
  `fulfillment-consumer-worker/`. One Deployment/Rollout + KEDA `ScaledObject`
  (rabbitmq trigger) **per `(type, subtype)` queue** for each of the two
  consumer roles (mirror today's per-method loop; iterate the registry in
  `values.yaml`).
- Secrets: drop `paymentsDatabaseUrl`; add `eventsDatabaseUrl`,
  `stripeSecretKey`, `stripeWebhookSecret`, `ticketSigningSecret`. Add an
  ingress path for `/api/v1/webhooks/payments/*`.
- Update `image.*` blocks for the renamed/new services.

### D3. Pulumi — **removed from the project**
`infra/pulumi/` has been deleted entirely, not just deprioritized. Helm + KIND
is the deploy/test path now (`infra/kind/`, `make k8s-apply`); Pulumi may come
back as its own initiative later if a real cloud target is needed again, but
it's out of scope for this pivot. CI's `deploy (pulumi up)` job was removed
from all three workflows (see D4) — no automated deploy exists right now.
Structural/comment references to Pulumi were scrubbed from the Helm chart,
`internal/infrastructure/{config,database}`, CI workflows, and the top-level
docs (CLAUDE.md/README.md/SECURITY.md/docs/runbook.md/loadtest/README.md) —
the latter two (SECURITY.md, runbook.md) got a "not currently provisioned by
anything in this repo" honesty pass rather than a full rewrite, since their
underlying AWS infra (KMS, RDS PITR/backup, network segmentation) has no
replacement yet.

### D4. CI (`.github/workflows/`)
Rename `consumer-worker.yml` → `order-consumer-worker.yml`; add
`fulfillment-consumer-worker.yml`; update triggering `cmd/<service>/**` paths. Keep the
gate order (build → lint incl. actionlint + helm lint → unit-tests → upload →
deploy; integration-tests optional/non-blocking).

### D5. Tests (`tests/integration/` + unit)
Rewrite the suite around the new pipeline. Replace `ingest_test`, `consumer_test`,
`card_test`, `pci_test`, `timescale_test` with:
- **order flow**: `POST /orders` → `order_outbox` NEW → relay → order queue →
  `order-consumer-worker` reserves tickets + `fake` gateway `CreateCheckout` + `Charge`
  PENDING.
- **fulfillment flow**: fake webhook → `payment_event_outbox` → relay → payment
  queue → `fulfillment-consumer-worker` → Charge PAID + Tickets VALID with **non-empty
  `qr_png`** and a verifiable `HMAC(ticketID, validation_code)` signature.
- **routing**: an order for each `(type, subtype)` lands on the right queue;
  an unknown pair is rejected `400`.
- **idempotency**: replayed order / redelivered webhook are safe no-ops.
- Unit: Stripe `VerifyWebhook` signature (good/bad sig), event-type registry
  validation, amount decimal→minor-units, PII redaction of `payment_details`.
- Suite container creates `outbox` + `events` DBs and applies both migration
  sets (no `AutoMigrate` in tests — dedicated test DB, per the rule).

### D6. k6 (`loadtest/`)
Rewrite: `k6-baseline.js`/`k6-autoscale.js` `POST /api/v1/orders` with a
randomized `(event_type, event_subtype)` spread across the registry (to exercise
all shards + KEDA per-queue scaling); replace `k6-consumer.js` with an
end-to-end script that also fires the **fake gateway webhook** so tickets get
issued under load. Keep `RATE_LIMIT_ENABLED=false` locally so k6 isn't
throttled.

### D7. Docs — scrub every payments-domain reference
Rewrite `CLAUDE.md` (new domain, four binaries, `outbox`+`events` DBs, type/
subtype routing, `PaymentGateway` port, removed payments), `README.md` (new
architecture diagram + flow), the Helm README, and
`.github/workflows/README.md`. Mark Phase 6 as **absorbed into Phase 7**.

**Completion criterion (user directive):** the repo must "forget everything
about payments" — `README.md`, `CLAUDE.md`, and **all files/comments** describe
an **Event Ticket System**, with **zero** residual payment-domain language
(payment methods, PIX/BOLETO/CARTAO/TRANSFER, `payments` DB/hypertables, the
payment wire format, `POST /api/v1/payments`). The **only** surviving payment
concept is the outbound `PaymentGateway` port + adapters used to charge orders.
Verify by grepping `payment`/`Payment`/method names and confirming every
remaining hit is the gateway port/adapter, not the old domain — this is a
sign-off gate, not a nice-to-have. Also purge `.claude/plan.md` and
`plan-phase2..6.md` premise references where they'd mislead (or add a
"superseded by Phase 7" banner).

---

## Files at a glance

**Create:** `cmd/fulfillment-consumer-worker/main.go`; `internal/domain/{producer,
location,event,order,charge,payment_gateway}.go`;
`internal/usecase/{order,webhook,checkout,fulfillment}/`;
`internal/adapter/paymentgateway/{stripe,fake,abacatepay,lemonsqueezy}/`;
`internal/adapter/ticketqr/`; `internal/adapter/http/{order_handler,order_dto,
webhook_handler,webhook_dto}.go`; `internal/adapter/messaging/{order_consumer,
payment_consumer}.go`; `migrations/events/000001_init.*`; new
`migrations/outbox/` init (`order_outbox` + `payment_event_outbox`); Helm
`templates/fulfillment-consumer-worker/*`; `.github/workflows/fulfillment-consumer-worker.yml`;
rewritten `tests/integration/*`; rewritten `loadtest/*`.

**Rename:** `cmd/consumer-worker` → `cmd/order-consumer-worker`;
`helm .../consumer-worker/` → `order-consumer-worker/`;
`.github/workflows/consumer-worker.yml` → `order-consumer-worker.yml`;
`observability/postgres/init-payments.sql` → `init-events.sql`.

**Modify:** `internal/infrastructure/rabbitmq/rabbitmq.go` (registry + topology),
`internal/usecase/outbox/` (generalized relay),
`internal/adapter/persistence/{outbox_repo,uow,models}.go` + new events repos,
`internal/adapter/messaging/publisher.go`,
`internal/infrastructure/config/config.go`, `internal/domain/{outbox,ticket,
pii}.go`, `cmd/{ingestion-api,outbox-worker,outbox-admin}/main.go`,
`docker-compose.yml`, `Makefile`, `infra/kind/*`, Helm `values.yaml`/secrets,
`go.mod`/`go.sum`, docs.

**Delete:** `internal/domain/{payment,schema}.go`,
`internal/adapter/http/{handler,dto,card}.go` (payments),
`internal/adapter/persistence/payment_repo.go`, `internal/usecase/{ingest,
consume}/`, `migrations/payments/*`, **`infra/pulumi/*` (entire directory)**.
(Fold Phase 6's planned `ticket_outbox` relay / `tickets` DB into this pivot —
do not build it separately.)

**Reuse unchanged:** `domain/{backoff,uow,uuid}.go`, `database.Listener`,
`telemetry`, `logging`, `observability`, `ratelimit`, the quorum-queue +
retry-TTL + explicit-reject-to-DLX topology pattern, publisher-confirm pattern,
manual-ack consumer pattern.

---

## Verification

1. `make build`, `make lint` (0 issues), `make test` (`-race`).
2. **Migrations**: fresh `make migrate` creates `outbox` (`order_outbox` +
   `payment_event_outbox`) and `events` (six tables, indexed — not
   partitioned, see A3's outcome note — on `event_type`/`event_subtype`);
   `payments` DB is gone.
3. **End-to-end (`make up`, `PAYMENT_PROVIDER=fake`)**: `POST /api/v1/orders`
   for `CONCERT`/`ROCK` → `order_outbox` NEW → `outbox-worker` PUBLISHED →
   `events.concert.rock.queue` → `order-consumer-worker` reserves tickets + Charge
   PENDING (fake checkout) → fake webhook `POST /api/v1/webhooks/payments/fake`
   → `payment_event_outbox` → `payments.concert.rock.queue` →
   `fulfillment-consumer-worker` → Charge PAID + Tickets VALID with non-empty `qr_png`;
   duplicate order / redelivered webhook are no-ops; an unknown
   `(type, subtype)` returns `400`.
4. **Integration test** asserts the full two-outbox pipeline + that
   `HMAC(ticketID, validation_code)` matches the stored `signature`, `qr_png`
   decodes, and Stripe `VerifyWebhook` rejects a bad signature.
5. `helm lint` + `helm template` render `order-consumer-worker` + `fulfillment-consumer-worker`
   Deployments and a `ScaledObject` per `(type, subtype)`; the KEDA outbox query
   sums both outbox backlogs.
6. **k6** drives `POST /orders` across all shards; per-queue KEDA scaling and
   ticket issuance hold under load.

## Notes / decisions

- **Routing = event category & genre** (`event_type` × `event_subtype`),
  replacing per-payment-method routing everywhere: queues, DLQs, retry queues,
  and the indexed (not partitioned — see A3's outcome note) domain tables.
- **Payment = outbound charge + inbound webhook**; **Stripe real**, `fake`
  sandbox for local/tests/k6, `abacatepay`/`lemonsqueezy` stubbed.
- **Payments domain fully replaced**; DBs become `outbox` + `events`. **This
  supersedes and absorbs Phase 6** (ticket relay + tickets DB) — Phase 6 does
  not ship on its own.
- **Two outboxes** (`order_outbox`, `payment_event_outbox`) keep ingestion-api
  broker-independent on both the write and webhook paths; the relay is
  generalized over a table + routing-key function.
- **DB sharding by type/subtype: index-only fallback chosen, not
  partitioning** (see A3's outcome note) — a `(type, subtype)` btree index
  makes "the database too" logically sharded without the `CREATE PARTITION`
  migration a real declarative-partitioning design would need per new
  `(type, subtype)` pair. Zero-migration extensibility won out over
  per-genre vacuum/retention independence at this system's current scale;
  revisit if that changes.
- **Four service binaries** (`ingestion-api`, `outbox-worker`, `order-consumer-worker`,
  `fulfillment-consumer-worker`) + `outbox-admin`. `order-consumer-worker` and
  `fulfillment-consumer-worker` could be one binary dispatching on message type; kept
  separate so each scales on its own backlog, consistent with the
  microservice-per-concern theme.
- Adding a new `(event_type, event_subtype)`: registry entry (+ queue declared)
  + optional partition migration + optional validation case — the small,
  localized change that per-method queues enjoy today.
- **Naming**: any service consuming from RabbitMQ must end in
  `-consumer-worker` (company convention) — `order-consumer-worker` and
  `fulfillment-consumer-worker`, not `order-worker`/`fulfillment-worker`.
  `outbox-worker` is exempt (it only publishes, never consumes).

## Part F — CI: Go vulnerability scanning

Add a `govulncheck` step to `lint` (or its own gate, `vulncheck`, right after
`lint`) in all four CI workflows (`ingestion-api.yml`, `outbox-worker.yml`,
`order-consumer-worker.yml`, `fulfillment-consumer-worker.yml`) — checks
`go.sum`'s dependency tree against the official Go vulnerability database
(https://vuln.go.dev) for known CVEs **actually reachable from this code's
call graph**, not just "a vulnerable version is present in go.sum" (which is
what a naive `go list -m all` diff would flag, with far more noise). Runs via
`golang.org/x/vuln/cmd/govulncheck` — `go run golang.org/x/vuln/cmd/govulncheck@latest ./...`,
no separate binary to pin/install, matching the `go run pkg@version` pattern
`cmd/outbox-admin`'s Makefile targets and `coverage-all`'s gocovmerge already
use. Fails the gate (non-zero exit) on any reachable vulnerability — same
blocking severity as `golangci-lint`/`actionlint`/`helm lint`, since a real
open-source-based CVE feed with new advisories landing regularly is exactly
the kind of check that must run on every PR, not just periodically. Add a
`- uses: actions/setup-go@v5` (already present) followed by
`- run: go run golang.org/x/vuln/cmd/govulncheck@latest ./...` inside the
existing `lint` job (cheapest — reuses that job's checkout/setup-go, one more
sequential step) rather than a new job, since it's a fast static analysis
with no test/build dependency of its own.

## Part G — Integration test coverage: every `internal/usecase/*` subpackage ≥ 75%

**Rule (added 2026-07-07):** each subpackage of `internal/usecase` —
`order`, `webhook`, `outbox`, `checkout`, `fulfillment` — must individually
clear **75%** statement coverage from `tests/integration`'s TestContainers
suite alone, not just an overall band across a wider set of packages. This
tightens the original goal below (a 65–70% band measured across a mixed
group of `usecase`/`adapter` packages) into a per-usecase-subfolder floor,
since the use-case layer is exactly the application-rule code this suite
exists to exercise end-to-end against a real Postgres + RabbitMQ.

Measure with `make test-integration` (already produces `integration.cov` via
`-coverpkg=./internal/... -coverprofile=integration.cov`) and a weighted
per-package rollup over the raw profile (statement-count weighted, not a
naive average of `go tool cover -func`'s per-function percentages — see the
one-off `awk` script used to produce the numbers below).

**Measured 2026-07-07 (`go test -tags=integration -race` against real
Postgres 18 + RabbitMQ 4.3 testcontainers, all 35 tests passing, overall
`internal/...` coverage 70.5%):**

| Package | Covered/total statements | Coverage |
|---|---|---|
| `usecase/outbox` | 58/67 | **86.6%** |
| `usecase/checkout` | 53/63 | **84.1%** |
| `usecase/order` | 36/44 | **81.8%** |
| `usecase/fulfillment` | 51/64 | **79.7%** |
| `usecase/webhook` | 26/34 | **76.5%** |

**All five `usecase` subpackages now clear 75%.** Closed via targeted tests
added specifically to reach the floor, not blanket padding:
  - `usecase/order`, `usecase/webhook`: added
    `TestPlaceOrder_UowExecuteError_ReturnsWrappedError` /
    `TestReceivePaymentEvent_UowExecuteError_ReturnsWrappedError`
    (`order_test.go`/`webhook_test.go`) — call the use-case directly with an
    already-canceled `context.Context` so the `UnitOfWork.Execute` transaction
    fails to even begin, exercising the real-DB-error wrapping branch that no
    HTTP-level test could reach (a duplicate/dedup hit is a *successful*
    `ON CONFLICT DO NOTHING`, not an error, so it never took this path).
  - `usecase/fulfillment`: added three poison-message tests to
    `fulfillment_test.go` mirroring `checkout_test.go`'s pattern —
    malformed JSON (`TestFulfillment_PoisonMessage_RoutesToDeadLetterQueue`),
    an unrecognized `schemaVersion`
    (`TestFulfillment_UnknownSchemaVersion_RejectsToDLQOnFirstAttempt`), and
    a well-formed message whose `providerRef` matches no `Charge` at all
    (`TestFulfillment_UnknownProviderRef_RoutesToDeadLetterQueue` — distinct
    from the existing `TestFulfillment_RedeliveredConfirmation_IsNoOp`, which
    covers an already-terminal Charge, not a missing one). Together these
    fully cover `recordRedactedError` (was 0%, no fulfillment test had ever
    driven a genuine processing error).
  - `usecase/outbox`: added two tests to `dispatch_outbox_test.go` —
    `TestDispatchOrderOutbox_NotifyTrigger_DispatchesFasterThanPollInterval`
    wires a real `database.Listener` as `Run`'s trigger channel (exactly how
    `cmd/outbox-worker/main.go` wires it) with a 10-minute poll interval, so
    a `PUBLISHED` row within 5s can only be explained by the NOTIFY/debounce
    path, not the ticker — exercising `Run`'s `trigger`/`debounceC` select
    cases for the first time. `TestDispatchOutbox_MetricsTicker_RecordsBacklogWithoutError`
    keeps one `DEAD_LETTER` and one `NEW` row alive across a 6-second `Run`,
    long enough for the (previously never-fired-in-any-test) 5-second metrics
    ticker to invoke `recordBacklogMetrics` — was 0%, real production code
    (`CountPending`/`CountDeadLetter`) that just never had a test patient
    enough to wait it out.

Two intentional, documented gaps remain (defensive branches, not missed
scenarios):
  - `uuid.NewV7()`/`json.Marshal` error branches in `usecase/order` and
    `usecase/webhook` — both fail only on conditions that don't occur in
    practice (crypto/rand exhaustion; marshaling a struct with no floats/
    channels/funcs), so they stay uncovered by design rather than via
    fault-injection contortions.
  - `usecase/fulfillment`'s `confirmAndIssue`/`failAndVoid` still have a few
    uncovered lines — the repository-call error branches inside them
    (`ticketRepo.MarkIssued`/`MarkVoid` failing mid-transaction) would need
    breaking the real DB mid-flight to trigger, which isn't worth the
    complexity for what's already comfortably over the 75% floor.

The broader `adapter`/`infrastructure` gaps noted in the original 65–70%-band
measurement (`AMQPPublisher.Publish`/`invalidate`, `GORMOutboxRepository`
untouched by the metrics-ticker fixes above) are unaffected by this rule —
it only binds `internal/usecase/*` — and remain fair game for a future pass
if broader `adapter` coverage becomes its own goal.
