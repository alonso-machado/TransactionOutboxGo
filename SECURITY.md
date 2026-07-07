# Security & PCI-DSS Posture

This document records the current security posture of the Transaction
Outbox system, with a focus on cardholder data (PAN) handling for PCI-DSS
relevance, and the single highest-priority gap to close before any real
productionization. See [`docs/runbook.md`](docs/runbook.md) for the
disaster-recovery procedures this posture's encryption/backup choices
support.

---

## 1. Cardholder data handling (PAN / CVV)

### PAN (card number) is masked at the earliest possible boundary

`internal/adapter/http/card.go`'s `maskPAN` rewrites the `cardNumber` field
inside the card sibling object (`cartao_credito`/`cartao_debito`) to its
last 4 digits **before the request ever reaches `ingest.Execute`**. This
means the full PAN:

- is **never persisted** to the `outbox_messages.payload` column,
- is **never published** to RabbitMQ,
- is **never written** to the `payments` table's `method_details` column,
- and is **never logged**, because every downstream component only ever
  sees the already-masked value.

As a second line of defense (in case a future code path bypasses the
handler-level mask, e.g. a raw payload echoed into an error message),
`internal/domain/pii.Redact`/`RedactJSON` independently mask any
`cardNumber` key — and `payerDocument`/`barcode`/`endToEndId`/`txid` for
the other payment methods' sensitive fields — wherever they appear in a
string or JSON document, including free-text log lines.

**Regression test**: [`tests/integration/pci_test.go`](tests/integration/pci_test.go)
(`TestPCI_CardPayment_NeverLeaksFullPANOrCVV`) posts a real card payment
end-to-end (ingest → outbox → RabbitMQ → consumer → `payments` table) and
asserts the full PAN never appears in the outbox payload, the raw RabbitMQ
message body, or the persisted row — only the last 4 digits. It also
independently re-confirms `pii.Redact` masks a literal PAN. This test is
the living assertion of the posture described in this section; if it ever
fails, the masking described above has regressed.

### CVV is never accepted, stored, or logged

The wire format's card sibling object (`CardDetailsDTO` in
`internal/adapter/http/dto.go`) has **no `cvv` field at all**. A client
that sends one is silently dropped by `json.Unmarshal` — there is no code
path anywhere in the system that reads, stores, publishes, or logs a CVV,
because the type the JSON is unmarshaled into has nowhere to put it. This
is enforced structurally, not by a runtime check, and is asserted by the
same PCI regression test above (it sends a `cvv` field and confirms it
never appears in any downstream artifact).

---

## 2. Encryption in transit

| Connection | Local/compose | Cloud target |
|---|---|---|
| App ↔ Postgres/RDS | plaintext (`sslmode=disable`) | `sslmode=require` (`internal/infrastructure/database/database.go`'s `withSSLMode`, driven by `DB_SSL_MODE`) |
| App ↔ RabbitMQ/Amazon MQ | plaintext (`amqp://`) | `amqps://` (`internal/infrastructure/rabbitmq/rabbitmq.go`'s `withAMQPS`, driven by `RABBITMQ_TLS`) |
| Client ↔ ingestion-api | plain HTTP locally | HTTPS via an ALB + ACM certificate (Phase 4 Track 1's ALB front door; cert provisioning is operator-managed) |

Both `DB_SSL_MODE` and `RABBITMQ_TLS` default to the plaintext local
posture so `make up`/the demo never breaks; the Helm chart's
`configMap.data` sets `DB_SSL_MODE: "disable"`/`RABBITMQ_TLS: "false"` as
its own explicit default too — a cloud values override is expected to flip
both for any real deployment, since Amazon MQ requires `amqps://` and RDS
should always be accessed over TLS for cardholder-adjacent traffic.

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
- **Single writer**: `consumer-worker` is the *only* component that writes
  to the `payments` table (Clean Architecture's port-interface boundary
  enforces this structurally — `ingestion-api` never imports
  `PaymentRepository`). This narrows the audit surface for "who touched
  cardholder-adjacent data" to one code path.
- **WAF**: the ALB front door (Phase 4 Track 1) is the attach point for an
  AWS WAF rate-based rule, with the in-process leaky-bucket rate limiter
  (`internal/adapter/http/ratelimit/`) as a second layer behind it.
- **Audit-log gap**: there is currently no centralized, tamper-evident
  audit log of "who/what called the ingestion API and when" beyond
  standard application logs (which themselves go through `pii.Redact`).
  A real PCI-scoped deployment should add structured, immutable audit
  logging (e.g. shipped to a write-once store) — this is the single
  largest compliance gap beyond the auth gap noted below.

## 5. Secrets management

Local/compose uses a static `.env` file and the Helm chart's plaintext
`templates/secret.yaml` (explicitly marked as a placeholder, never to hold
real credentials). A cloud target can source `DATABASE_URL`/`RABBITMQ_URL`
from **AWS Secrets Manager** via the **External Secrets Operator** (ESO),
authenticating with **IRSA** — no static AWS credentials would then live
anywhere in the cluster (`helmcharts/transaction-outbox/templates/
externalsecret.yaml`, gated by `externalSecrets.enabled`) — but ESO itself
and its IRSA role must be installed separately; the Pulumi program that used
to do that (`infra/pulumi/`) was removed. ESO's `ExternalSecret` CR re-syncs every 5
minutes (`refreshInterval`), so a Secrets Manager-side credential rotation
propagates to the cluster's `Secret` automatically, though pods must still
be restarted (or watch the mounted Secret for changes, which this app does
not currently do) to actually pick up new connection strings — see
[`docs/runbook.md`](docs/runbook.md) §3 for the exact rollout step.

---

## 6. The single highest-priority gap: no authentication on the ingestion API

**`POST /api/v1/payments` has no authentication or signature verification
today.** Any caller that can reach the ALB can submit a payment event. This
is the most significant gap relative to a real payment-provider webhook
integration, where the provider signs each delivery (e.g. an HMAC over the
raw body using a shared secret) so the receiver can verify the request
genuinely originated from the provider and wasn't tampered with in
transit.

**If this system were productionized, HMAC signature verification on
inbound webhook deliveries is the first thing to add** — verifying a
provider-supplied signature header (e.g. `X-Signature: sha256=<hmac>`
computed over the raw request body with a per-provider shared secret)
before any payload parsing happens, rejecting unsigned or invalid requests
with `401`. This is deliberately **not implemented** in the current system
because:

- it is orthogonal to the Transactional Outbox pattern that is the actual
  subject of this project,
- and faking a "real" provider's signature scheme without an actual
  provider integration would add complexity without adding to the
  pattern's own demonstration value.

Everything else in this document — TLS, encryption at rest, secrets
rotation, PAN masking, network segmentation — is genuine production
hardening already present. Authentication on the write path is the one
deliberate, documented exception, and should be the first thing addressed
before this system (or anything derived from it) ever accepts real
payment-provider traffic.
