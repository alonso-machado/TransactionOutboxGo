# Security Posture

This document records the current security posture of the Event Ticket
System, with a focus on payment data handling and the single
highest-priority gap to close before any real productionization. See
[`docs/runbook.md`](docs/runbook.md) for the disaster-recovery procedures
this posture's encryption/backup choices support.

---

## 1. Payment data handling: no cardholder data ever reaches this system

Unlike a system that accepts card numbers directly, this project's
`PaymentGateway` port (`internal/domain/payment_gateway.go`) delegates the
entire charge to the provider's own **hosted checkout page**
(`CreateCheckout` returns a `checkoutUrl` the customer is redirected to).
Card number, CVV, and any other cardholder data are typed directly into the
provider's page — Stripe's, for a real deployment — and never transit this
system's HTTP handlers, database, or message broker in any form.

Concretely:

- The order wire format (`POST /api/v1/orders`) has no card fields at all —
  only `customer.name`/`email`/`document` and per-ticket price/section
  data. There is no code path anywhere in the system that could read,
  store, publish, or log a PAN or CVV, because no type in the request or
  domain model has anywhere to put one.
- The inbound webhook (`POST /api/v1/webhooks/payments/{provider}`) carries
  only an **outcome** (`CONFIRMED`/`FAILED`) plus the provider's own
  reference id — again, no cardholder data.
- `internal/domain/pii.Redact`/`RedactJSON` still exist as a defense-in-depth
  layer over free-text log lines and error messages, now scoped to PII
  relevant to this domain — `email`, `document`, `validationCode`,
  `signature` — rather than card fields, since there are no card fields left
  to redact.

**Practical effect on PCI-DSS scope:** with card data handled entirely by
the provider's own hosted checkout (SAQ A-eligible integration pattern),
this system itself carries materially less PCI scope than a system that
touches a PAN directly. This is a property of the architecture (delegating
to a hosted checkout), not a control this repo actively enforces — the
scoping conclusion still needs a real compliance review before being relied
upon, and holds only as long as every gateway adapter keeps using a hosted
checkout flow rather than accepting raw card fields itself.

---

## 2. Encryption in transit

| Connection | Local/compose | Cloud target |
|---|---|---|
| App ↔ Postgres/RDS | plaintext (`sslmode=disable`) | `sslmode=require` (`internal/infrastructure/database/database.go`'s `withSSLMode`, driven by `DB_SSL_MODE`) |
| App ↔ RabbitMQ/Amazon MQ | plaintext (`amqp://`) | `amqps://` (`internal/infrastructure/rabbitmq/rabbitmq.go`'s `withAMQPS`, driven by `RABBITMQ_TLS`) |
| Client/gateway ↔ ingestion-api | plain HTTP locally | HTTPS via an ALB + ACM certificate (cert provisioning is operator-managed) |
| ingestion-api ↔ payment gateway | HTTPS (the gateway SDK, e.g. `stripe-go`, always talks TLS regardless of environment) | same |

Both `DB_SSL_MODE` and `RABBITMQ_TLS` default to the plaintext local
posture so `make up`/the demo never breaks; the Helm chart's
`configMap.data` sets `DB_SSL_MODE: "disable"`/`RABBITMQ_TLS: "false"` as
its own explicit default too — a cloud values override is expected to flip
both for any real deployment, since Amazon MQ requires `amqps://` and RDS
should always be accessed over TLS.

## 3. Encryption at rest

RDS storage should be encrypted at rest via a dedicated customer-managed KMS
key (with key rotation enabled) rather than the default AWS-managed `aws/rds`
key — this makes key ownership and rotation explicit and auditable rather
than implicit. **Not currently provisioned by anything in this repo** — the
Pulumi program that used to set this up (`infra/pulumi/`) was removed; the
Helm chart alone doesn't provision RDS. EBS volumes backing EKS worker nodes
inherit the cluster's default EKS-managed encryption (not separately
configured here).

## 4. Network segmentation & audit

- **Private subnets**: RDS and Amazon MQ should be `PubliclyAccessible:
  false` and reachable only from the EKS node security group — neither
  internet-reachable, let alone reachable from outside the cluster's own
  worker nodes. **Not currently provisioned by anything in this repo** —
  this was Pulumi's job (`infra/pulumi/`, now removed); enforcing it is on
  whatever provisions the cluster/RDS/Amazon MQ next.
- **Single writer per database**: `order-consumer-worker` and
  `fulfillment-consumer-worker` are the *only* components that write to the
  `events` database (Clean Architecture's port-interface boundary enforces
  this structurally — `ingestion-api` never imports `OrderRepository`/
  `TicketRepository`/`ChargeRepository`). This narrows the audit surface for
  "who touched order/ticket/charge data" to two code paths, both of which
  only ever act on a message the outbox relay itself published.
- **WAF**: the ALB front door is the attach point for an AWS WAF rate-based
  rule, with the in-process leaky-bucket rate limiter
  (`internal/adapter/http/ratelimit/`) as a second layer behind it — see
  [README.md's Edge protection section](README.md#edge-protection-rate-limiting--waf).
- **Audit-log gap**: there is currently no centralized, tamper-evident
  audit log of "who/what called the ingestion API and when" beyond
  standard application logs (which themselves go through `pii.Redact`).
  A real production deployment should add structured, immutable audit
  logging (e.g. shipped to a write-once store) — this is the single
  largest compliance gap beyond the auth gap noted below.

## 5. Secrets management

Local/compose uses a static `.env` file and the Helm chart's plaintext
`templates/secret.yaml` (explicitly marked as a placeholder, never to hold
real credentials — this includes `stripeSecretKey`/`stripeWebhookSecret`/
`ticketSigningSecret` alongside the database/broker URLs). A cloud target
can source these from **AWS Secrets Manager** via the **External Secrets
Operator** (ESO), authenticating with **IRSA** — no static AWS credentials
would then live anywhere in the cluster
(`helmcharts/transaction-outbox/templates/externalsecret.yaml`, gated by
`externalSecrets.enabled`) — but ESO itself and its IRSA role must be
installed separately; the Pulumi program that used to do that
(`infra/pulumi/`) was removed. ESO's `ExternalSecret` CR re-syncs every 5
minutes (`refreshInterval`), so a Secrets Manager-side credential rotation
propagates to the cluster's `Secret` automatically, though pods must still
be restarted (or watch the mounted Secret for changes, which this app does
not currently do) to actually pick up new connection strings — see
[`docs/runbook.md`](docs/runbook.md) §3 for the exact rollout step.

---

## 6. The single highest-priority gap: no authentication on order placement

**`POST /api/v1/orders` has no authentication today.** Any caller that can
reach the ALB can submit an order. Unlike the payment-gateway webhook route
(see below), there is no signature or API-key scheme protecting it.

By contrast, **`POST /api/v1/webhooks/payments/{provider}` is already
signature-verified** — the handler calls the configured
`PaymentGateway.VerifyWebhook` before any payload is trusted, and the real
`stripe` adapter uses Stripe's own `webhook.ConstructEvent`, which rejects a
request whose `Stripe-Signature` header doesn't match an HMAC computed over
the raw body with `STRIPE_WEBHOOK_SECRET`. The `fake` adapter (the
local/test default) does **not** enforce a real signature check — by
design, since it never talks to a real provider — so this protection only
applies once `PAYMENT_PROVIDER=stripe` (or a future real-provider adapter)
is actually configured.

**If this system were productionized, the first thing to add is
authentication on order placement** — e.g. an API key or a signed request
scheme identifying which storefront/frontend is placing the order, rejecting
unauthenticated requests with `401`. This is deliberately **not
implemented** in the current system because it is orthogonal to the
Transactional Outbox pattern that is the actual subject of this project.

Everything else in this document — TLS, encryption at rest, secrets
rotation, hosted-checkout card-data avoidance, network segmentation, and
webhook signature verification — is genuine hardening already present.
Authentication on the order-placement write path is the one deliberate,
documented exception, and should be the first thing addressed before this
system (or anything derived from it) ever accepts real customer traffic.
