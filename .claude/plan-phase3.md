# Plan Phase 3 — Per-Method Queues, Card Payments (PII), CI/CD & Pulumi/AWS

> **When to execute:** after Phase 2 is complete and the stack runs end-to-end
> with OTel, Swagger, TestContainers and the K8s + KEDA manifests in place.
> **Prerequisite:** `.claude/plan-phase2.md` fully implemented; `make up`,
> `make test-integration` and `make k8s-apply` all pass.

---

## Overview

Five tracks. Recommended execution order — each builds on the previous:

| # | Track | Why this order |
|---|---|---|
| 1 | **Per-method RabbitMQ queues** | The routing/topology change is the structural core of Phase 3; everything else (consumers, KEDA, CI, infra) targets the new shape |
| 2 | **Card payment methods + PII** | New methods (`CARTAO_CREDITO`, `CARTAO_DEBITO`) flow through the per-method queues from track 1 and add a card-number PII surface |
| 3 | **GitHub Actions CI/CD** | Build → unit tests (gate) → golangci-lint (gate) → optional TestContainers — locks quality before any deploy |
| 4 | **Pulumi / AWS (EKS)** | Provision the cloud target the CI pipeline deploys to; reuses the K8s + KEDA manifests from Phase 2 |
| 5 | **Cross-cutting adjustments** | Tests, docs, config and observability updates that span the tracks |
| 6 | **k6 load tests** | Run last — three scripts: P95/P99 latency baseline (pinned 1/queue), autoscaling test (KEDA `min 0`/`max 10`, scale-to-zero), and a consumer-worker test (xk6-amqp publish → DB observe). Needs tracks 1–2 and a deployed target |

**Guiding principle (unchanged):** the Clean Architecture dependency rule still
holds. `internal/domain` and `internal/usecase` must not import Gin, GORM or
`amqp091-go`. The per-method routing key is a domain concept (a property of the
`OutboxMessage`); the *mapping* of that key onto an AMQP exchange/queue stays in
`adapter/messaging` + `infrastructure/rabbitmq`.

---

## Track 1 — Per-Method RabbitMQ Queues

### Goal

Today there is **one** queue (`payments.queue`) and **one** consumer-worker
deployment draining it for every payment method. Phase 3 decouples this: each
payment method gets its **own** durable quorum queue, and each consumer-worker
instance binds to and consumes from **exactly one** queue. This lets KEDA scale
each method independently on its own queue depth (a spike in PIX traffic must not
force CARD consumers to scale, and vice-versa).

### Decisions

- **Exchange stays a single topic exchange** `payments.exchange` (durable). No
  need for multiple exchanges — the topic routing key carries the method.
- **Routing key becomes method-derived:** `payment.<method-lowercased>`, e.g.
  `payment.pix`, `payment.boleto`, `payment.transfer`, `payment.cartao_credito`,
  `payment.cartao_debito`. (Replaces the single static `payment.created`.)
- **One queue per method:** `payments.<method>.queue` (quorum), each bound to the
  exchange with its own routing key:
  - `payments.pix.queue`           ← `payment.pix`
  - `payments.boleto.queue`        ← `payment.boleto`
  - `payments.transfer.queue`      ← `payment.transfer`
  - `payments.cartao_credito.queue`← `payment.cartao_credito`
  - `payments.cartao_debito.queue` ← `payment.cartao_debito`
- **One DLQ per method:** `payments.<method>.dlq` bound to a shared DLX
  `payments.dlx` with routing key `payment.<method>.dead`. Keeps poison-message
  isolation per method (a poison CARD message never pollutes the PIX DLQ).
- **The publisher (`DispatchOutbox`) needs no per-method config** — it derives the
  routing key from the outbox row's method and publishes to the single topic
  exchange. The broker fans the message into the correct queue.
- **Each consumer-worker process consumes ONE queue**, selected by a new env var
  `PAYMENT_QUEUE` (e.g. `payments.pix.queue`). No queue env → fail fast at
  startup (no implicit "consume everything" mode — that would defeat the
  decoupling).
- **Topology ownership:** the **ingestion-api** declares the *full* topology (all
  method queues + bindings + DLX/DLQs) on startup, because its `DispatchOutbox`
  publishes to all of them. Each **consumer-worker** idempotently re-declares
  *only its own* queue + DLQ before consuming (declare is idempotent, so this is
  safe and makes a worker self-sufficient if started first).
- **Method → routing-key normalization is centralized** in one helper so the HTTP
  boundary, the publisher and the topology declaration all agree:
  `routingKeyFor(method) = "payment." + strings.ToLower(method)`.

### Why carry the method on the OutboxMessage (not parse the payload)

The publisher must stay ignorant of the payload's internal JSON shape (Clean
Architecture: `adapter/messaging` depends on the `domain.OutboxMessage` port, not
on ingest's private `outboxPayload` struct). So the method travels as a
first-class field on the domain entity.

### Domain change

Add a `PaymentMethod` field to `domain.OutboxMessage` (plain string, no framework
import) — it already carries `AggregateType`, `HTTPMethod`, `Route`; the method is
the same kind of routing metadata:

```go
type OutboxMessage struct {
    // ... existing fields ...
    PaymentMethod string // e.g. "PIX" — drives the per-method routing key
}
```

Persist it as a column on `outbox_messages` (`payment_method TEXT NOT NULL`) in
the GORM model (`adapter/persistence`) so it survives a restart between enqueue
and dispatch. AutoMigrate handles the new column for local dev.

### Files to add / modify

| File | Change |
|---|---|
| `internal/domain/outbox.go` | Add `PaymentMethod` field to `OutboxMessage` |
| `internal/infrastructure/rabbitmq/rabbitmq.go` | Replace single-queue constants with a `Methods` list + `QueueFor`/`DLQFor`/`RoutingKeyFor`/`DLXRoutingKeyFor` helpers; rewrite `DeclareTopology` to loop over methods; add `DeclareQueue(ch, method)` for a single-queue declare |
| `internal/adapter/messaging/publisher.go` | Publish with `rmq.RoutingKeyFor(msg.PaymentMethod)` instead of the static `rmq.RoutingKey` |
| `internal/adapter/messaging/consumer.go` | `NewConsumer` takes a `queueName string`; `Run` declares + consumes that queue; retry republish uses the per-method routing key (so a requeued message returns to the same queue) |
| `internal/usecase/ingest/ingest.go` | Set `msg.PaymentMethod = req.Method` on the `OutboxMessage` |
| `internal/adapter/persistence/*` | Add `PaymentMethod` to the outbox GORM model + repo mapping |
| `internal/infrastructure/config/config.go` | Add `PaymentQueue string \`envconfig:"PAYMENT_QUEUE" required:"true"\`` (consumer-worker only — required so a misconfigured worker fails fast) |
| `cmd/consumer-worker/main.go` | Read `cfg.PaymentQueue`; pass to `NewConsumer`; declare only that queue |
| `cmd/ingestion-api/main.go` | `DeclareTopology` already declares everything — no behavioural change beyond the new helpers |

### Known methods list

A canonical list lives in `infrastructure/rabbitmq` (the topology owner):

```go
var Methods = []string{"PIX", "BOLETO", "TRANSFER", "CARTAO_CREDITO", "CARTAO_DEBITO"}
```

> **Note on "unknown methods":** Phase 1's design allows arbitrary `method`
> values to pass `ValidateMethod` unvalidated. With per-method queues, a method
> *not* in `Methods` has **no bound queue** and would be dropped by the broker
> (published to a topic exchange with no matching binding = silently discarded).
> Two options — **decision: option (a)**:
> - **(a) Reject at ingest** if `method` is not in `Methods` (return 400). This is
>   the safe default for Phase 3: no silently-dropped messages. Adding a method
>   becomes a deliberate 2-line change (add to `Methods` + optionally a
>   `ValidateMethod` case).
> - (b) Add a catch-all `payments.other.queue` bound with `payment.*` for the
>   long tail. Documented as a future option, not built now.

### Verification

1. `make up` (compose now starts one consumer per method — see Track 5) →
   RabbitMQ UI shows 5 queues + 5 DLQs.
2. `make seed` (PIX) → message appears only in `payments.pix.queue`; PIX consumer
   persists it; other queues stay at 0.
3. POST a BOLETO → lands only in `payments.boleto.queue`.
4. POST a method not in `Methods` → `400` (decision a), nothing published.

---

## Track 2 — Card Payment Methods (`CARTAO_CREDITO`, `CARTAO_DEBITO`) + PII

### Goal

Add two first-class card methods. The card number (PAN) is PII and must **never**
appear in logs, error responses, trace spans, **or be persisted in full**.

### Card wire format

Following the existing polymorphic convention (the method-specific sub-object is a
top-level sibling key named after `payment.method` lowercased), the two methods
carry a sibling object named `cartao_credito` / `cartao_debito` respectively, with
**exactly three fields**:

```jsonc
{
  "eventId": "...",
  "provider": { "name": "...", "providerPaymentId": "..." },
  "payment": { "paymentId": "...", "amount": 100.50, "currency": "BRL", "method": "CARTAO_CREDITO" },
  "cartao_credito": {
    "cardNumber": "4111111111111111",   // PAN — PII, masked before storage/logging
    "cardType":   "CREDIT",             // CREDIT | DEBIT  (must match the method)
    "cardIssuer": "VISA"                // VISA | MASTERCARD | AMERICAN
  },
  "occurredAt": "..."
}
```

### Decisions

- **One shared `CardDetailsDTO`** validates both card methods (the sibling key
  differs, the validation is identical). Validation rules:
  - `cardNumber` required, non-empty, digits only (length 13–19 — standard PAN
    range). Validated structurally; the value itself is never echoed in an error.
  - `cardType ∈ {CREDIT, DEBIT}` **and must be consistent with the method**
    (`CARTAO_CREDITO ⇒ CREDIT`, `CARTAO_DEBITO ⇒ DEBIT`) — a mismatch is a 400.
  - `cardIssuer ∈ {VISA, MASTERCARD, AMERICAN}` (exactly these three).
- **PAN masking at the HTTP boundary (decision: mask-then-store):** before the
  card details are ever placed into `MethodDetails` (the JSONB stored opaquely and
  carried on the outbox/RabbitMQ message), the handler rewrites `cardNumber` to
  **last-4 only** (`************1111`). Rationale: the full PAN is never written to
  Postgres, never published to RabbitMQ, never logged. This sidesteps PCI-scope
  creep entirely — we keep only what's needed to identify the card to a human.
  - The amount/UUID boundary conversion already establishes the pattern: *the HTTP
    boundary is where wire-format values get normalized before the inner layers
    see them.* PAN masking joins that boundary.
  - If a future requirement needs the full PAN (e.g. to call an acquirer), the
    correct design is tokenization via a vault, **not** storing the PAN — noted as
    a hardening item, out of scope here.
- **`pii.Redact` gains `cardNumber`** as a defense-in-depth second layer: even
  though the PAN is masked at the boundary, `cardNumber` is added to the PII key
  set so any accidental leak (a raw body logged before masking, a future code
  path) is still redacted in logs/errors/traces.

### Files to add / modify

| File | Change |
|---|---|
| `internal/adapter/http/dto.go` | Add `CardDetailsDTO{CardNumber, CardType, CardIssuer}` + `Validate(method)`; add `CARTAO_CREDITO`/`CARTAO_DEBITO` cases to `ValidateMethod` (look up sibling by lowercased method name, validate type↔method consistency + issuer enum) |
| `internal/adapter/http/handler.go` | After validation, if the method is a card method, mask the PAN to last-4 inside the extracted `methodDetails` before passing it to `ingest.Execute` |
| `internal/adapter/http/card.go` *(new)* | `maskPAN(raw json.RawMessage) (json.RawMessage, error)` — rewrites `cardNumber` → last-4; pure, unit-tested |
| `internal/domain/pii/redact.go` | Add `"cardnumber"` to `sensitiveKeys` and `cardNumber` to `keyValuePattern` |
| `internal/infrastructure/rabbitmq/rabbitmq.go` | Add `CARTAO_CREDITO`, `CARTAO_DEBITO` to `Methods` (so their queues/bindings are declared) |
| `docs/` (swagger) | Re-run `make swag` so `CardDetailsDTO` + the two methods appear in the OpenAPI spec |

> **No change to `domain.Payment`** — `Method` is already a free string and
> `MethodDetails []byte` already stores the (now masked) card object opaquely.
> This is exactly the polymorphic-method design paying off: adding a method needs
> a new `ValidateMethod` case and a queue binding, not a schema migration.

### Verification

1. POST `CARTAO_CREDITO` with `cardType: "DEBIT"` → `400` (type/method mismatch).
2. POST `CARTAO_CREDITO` with `cardIssuer: "ELO"` → `400` (issuer not in enum).
3. POST a valid `CARTAO_CREDITO` → `201`; inspect `outbox_messages.payload` and
   the consumed `payments.method_details` → `cardNumber` shows only last-4; full
   PAN appears **nowhere** in Postgres.
4. Force a processing error on a card message → the DLQ message + the log line +
   the trace span show `cardNumber: "***"` (redaction), never the digits.
5. Message routes only to `payments.cartao_credito.queue`.

---

## Track 3 — GitHub Actions CI/CD

### Goal

A pipeline that **gates** on quality before any deploy: every push/PR builds,
then runs unit tests (stop on failure), then runs golangci-lint (stop on
failure), with an **optional** TestContainers integration stage. Image build +
deploy only run after the gates pass on the default branch.

> **Note on the host-Podman convention:** the "no `go` on the host, run everything
> through Podman" rule is a *local-dev* convention for the user's machine. CI
> runs on GitHub-hosted `ubuntu-latest`, which has Go, Docker and the toolchain
> natively — so the workflow uses `actions/setup-go` and the official lint action
> directly. This is intentional and does not violate the project rule (which is
> about the dev host, not CI).

### Pipeline shape

`.github/workflows/ci.yml` — triggered on `push` and `pull_request`. The three
mandatory gates run **strictly in this order** — any one failing **terminates the
CI/CD** and nothing downstream runs:

```
build  →  lint (GATE)  →  unit-tests (GATE)  →  docker  →  deploy
                                   └──────────►  integration-tests (OPTIONAL, off the deploy path)
```

- **`build`** (gate 1) — `go build ./...` (the same compilation `make build`
  does). Fails ⇒ stop.
- **`lint`** (gate 2) — `golangci/golangci-lint-action@v6`, `needs: build`.
  Mirrors the local `make lint` zero-issues rule. A finding **fails the workflow**
  ⇒ stop. **Runs before the unit tests**, per requirement.
- **`unit-tests`** (gate 3) — `go test -race ./internal/...` (the non-`integration`
  tagged tests), `needs: lint`. A failure **fails the workflow** ⇒ stop. **Runs
  after lint.**
- **`integration-tests` (optional, NOT a deploy gate)** — runs the Phase 2
  TestContainers suite (`go test -tags=integration ...`), `needs: unit-tests`. It
  is a **side branch**: it never blocks `docker`/`deploy`. Optional via:
  - `workflow_dispatch` boolean input `run_integration` (manual runs), **or**
  - a `ci:integration` PR label / push to the default branch.
  - Skipped on ordinary PRs by default (keeps PR feedback fast; the suite needs
    Docker and is slow). `ubuntu-latest` has Docker, so TestContainers/Ryuk work
    with no extra service config.
- **`docker`** — multi-arch build of both services via `build/Dockerfile`
  (`ARG SERVICE`), push to the registry (ECR — see Track 4), tag = git SHA.
  `needs: unit-tests`; runs only on the default branch after the 3 gates pass.
- **`deploy` (last)** — runs `pulumi up` (Track 4). Comes **right after the unit
  tests** in the chain (`unit-tests → docker → deploy`); it does **not** wait on
  the optional integration suite. Prod gated by an environment protection rule.

### Gating semantics (explicit, per the requirements)

- **Order is build → lint → unit-tests**, enforced by the `needs` chain
  (`lint: needs build`, `unit-tests: needs lint`). Each is a hard gate: a red job
  stops everything after it.
- **Integration TestContainers is optional and off the deploy path** —
  `needs: unit-tests` only so it reuses the gate result, but nothing `needs` it,
  so it can be skipped/fail without blocking deploy.
- **Deploy is last, right after unit-tests** — the deploy path is
  `unit-tests → docker → deploy`; it depends on the unit-test gate (transitively
  on lint+build), never on integration.

### Files to add

| File | Purpose |
|---|---|
| `.github/workflows/ci.yml` | The pipeline above |
| `.github/workflows/README.md` *(optional)* | One-paragraph description of the gates + how to trigger the optional integration run |
| `.golangci.yml` | **Only if needed** to pin the linter version for CI reproducibility — *no rule-silencing overrides* (the project rule: fix code, never suppress). Default config preferred |

### Sketch (`ci.yml`)

```yaml
name: CI
on:
  push: { branches: [main] }
  pull_request:
  workflow_dispatch:
    inputs:
      run_integration:
        description: "Run TestContainers integration suite"
        type: boolean
        default: false

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with: { go-version: "1.26" }
      - run: go build ./...

  lint:                 # GATE 2 — after build, before unit-tests
    needs: build
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with: { go-version: "1.26" }
      - uses: golangci/golangci-lint-action@v6

  unit-tests:           # GATE 3 — after lint
    needs: lint
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with: { go-version: "1.26" }
      - run: go test -race ./internal/...

  integration-tests:    # OPTIONAL — side branch, never gates deploy
    needs: unit-tests
    if: >-
      github.event_name == 'push' ||
      (github.event_name == 'workflow_dispatch' && inputs.run_integration) ||
      contains(github.event.pull_request.labels.*.name, 'ci:integration')
    runs-on: ubuntu-latest   # Docker is available → TestContainers works
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with: { go-version: "1.26" }
      - run: go test -tags=integration -race -timeout=300s ./tests/integration/...

  docker:               # after unit-tests gate
    needs: unit-tests
    if: github.ref == 'refs/heads/main'
    runs-on: ubuntu-latest
    # build+push ingestion-api and consumer-worker to ECR, tag = ${{ github.sha }}

  deploy:               # LAST — right after unit-tests (via docker), not after integration
    needs: docker
    if: github.ref == 'refs/heads/main'
    environment: dev      # prod gated by environment protection rules
    runs-on: ubuntu-latest
    # pulumi up (Track 4)
```

### Verification

1. Introduce a lint finding → `lint` red → `unit-tests`, `docker`, `deploy` never
   run (lint is gate 2, before unit tests).
2. Open a PR with a failing unit test (lint clean) → `unit-tests` red →
   `docker`/`deploy` skipped.
3. Add the `ci:integration` label → `integration-tests` runs the TestContainers
   suite, but a failure there does **not** block `deploy` (optional, off-path).
4. Merge to `main` with the 3 gates green → `docker` pushes SHA-tagged images →
   `deploy` runs `pulumi up` (last).

---

## Track 4 — Pulumi (AWS / EKS)

### Goal

Provision the AWS target the CI pipeline deploys to, as code. Reuse the Phase 2
K8s + KEDA manifests rather than re-authoring them — Pulumi stands up the cluster
and managed dependencies, installs KEDA, and applies (or templates) the existing
`k8s/` manifests.

### Why EKS (not ECS)

Phase 2 already built `k8s/` manifests **and KEDA ScaledObjects**. KEDA is a
Kubernetes-native autoscaler — it does not run on ECS. The per-method KEDA scaling
from Track 1 is the whole point of Phase 3's queue split, so the AWS target must
be Kubernetes ⇒ **EKS**. (ECS Fargate would mean re-implementing queue-depth
autoscaling with Application Auto Scaling + custom CloudWatch metrics, discarding
the KEDA work — rejected.)

### Decisions

- **Pulumi language: Go.** Keeps the monorepo single-language; the infra code can
  share types/constants (e.g. the `Methods` list, queue names) with the app by
  importing `internal/infrastructure/rabbitmq` constants where useful. Lives in
  `infra/pulumi/`.
- **Stacks:** `dev` and `prod` (`Pulumi.dev.yaml`, `Pulumi.prod.yaml`). Secrets via
  Pulumi's encrypted config (`pulumi config set --secret`), backed by AWS KMS.
- **Components provisioned:**
  | Component | AWS resource | Notes |
  |---|---|---|
  | Network | VPC + public/private subnets + NAT | `pulumi-eks`/`awsx` higher-level component for brevity |
  | Cluster | **EKS** + managed node group | Or Fargate profiles; node group default for KEDA/long-running consumers |
  | Registry | **ECR** repos: `ingestion-api`, `consumer-worker` | CI pushes SHA-tagged images here (Track 3) |
  | Database | **RDS PostgreSQL 17** | Multi-AZ in prod; security group allows EKS nodes only |
  | Broker | **Amazon MQ for RabbitMQ** | Managed; exposes the management HTTP API that KEDA's `rabbitmq` trigger needs (matches `scaledobject.yaml`'s `protocol: http`). **Decision: Amazon MQ over SQS** — see below |
  | Autoscaler | **KEDA** via Helm release | `pulumi-kubernetes` `helm.Release` of the KEDA chart into the cluster |
  | Secrets | **AWS Secrets Manager** (DB DSN, RabbitMQ mgmt URL) | Injected into pods via env / External Secrets; replaces the placeholder `k8s/secret.yaml` |
  | Workloads | The Phase 2 `k8s/` manifests | Applied via `pulumi-kubernetes` `ConfigGroup`/`kustomize`, **one consumer Deployment + ScaledObject per method** (Track 1) |
- **Per-method consumer deployments:** Track 1 turned one consumer into N. Pulumi
  generates the N Deployments + N KEDA ScaledObjects by looping over the `Methods`
  list — each Deployment sets `PAYMENT_QUEUE=payments.<method>.queue` and its
  ScaledObject's `queueName` to the same, so each method scales independently
  (incl. scale-to-zero). This is the cloud realization of the Phase 3 goal.
- **KEDA tuning per method (Phase 3 values):** every per-method `ScaledObject` is
  set to **`minReplicaCount: 0`** (scale-to-zero when the queue is idle),
  **`maxReplicaCount: 10`** (hard cap of 10 pods per queue), and a RabbitMQ
  `QueueLength` trigger with **`value: "1000"`** — i.e. KEDA targets ~1000 queued
  messages per replica, so a queue must climb past 1000 before a 2nd pod is added,
  and it tops out at 10 pods (≈10 000+ backlog). These supersede the Phase 2
  placeholder (`max 20`, `value 100`). This is the configuration the autoscaling
  k6 test (Track 6, scenario 2) exercises.
- **Amazon MQ vs SQS (decided: Amazon MQ):** SQS is cheaper (serverless, ~zero
  cost when idle, native DLQ redrive, native KEDA `aws-sqs-queue` scaler), but it
  is **not AMQP** — adopting it means **rewriting the entire `adapter/messaging` +
  `infrastructure/rabbitmq` layer** (no topic exchange, quorum queues, publisher
  confirms, DLX, or the `x-retry-count` retry mechanism). To preserve the existing
  RabbitMQ-based outbox implementation **with zero rewrite**, the plan uses
  **Amazon MQ for RabbitMQ**. Trade-off accepted: a managed broker runs 24/7
  (~US$20+/month even idle; it does **not** scale cost to zero) in exchange for the
  AMQP code staying untouched. SQS remains a documented future option if cloud
  cost becomes the priority over keeping the RabbitMQ showcase.
- **Amazon MQ vs RabbitMQ-on-EKS:** within the RabbitMQ path, managed Amazon MQ is
  chosen for the dev path (less to operate, native mgmt API for KEDA). The
  RabbitMQ Cluster Operator on EKS is noted as the prod-grade alternative (matches
  `k8s/rabbitmq/NOTE.md`) but not built now.

### File structure

```
infra/
└── pulumi/
    ├── Pulumi.yaml                # project (runtime: go)
    ├── Pulumi.dev.yaml            # dev stack config (non-secret)
    ├── Pulumi.prod.yaml           # prod stack config
    ├── main.go                    # composition: network → cluster → data → workloads
    ├── network.go                 # VPC/subnets
    ├── cluster.go                 # EKS + node group + ECR
    ├── data.go                    # RDS Postgres + Amazon MQ RabbitMQ + Secrets Manager
    ├── keda.go                    # KEDA Helm release
    ├── workloads.go               # apply k8s/ manifests; generate per-method consumer Deployment+ScaledObject
    └── README.md                  # how to `pulumi up`, required config/secrets, AWS creds
```

### Config / secrets the stack expects

```bash
pulumi config set aws:region us-east-1
pulumi config set --secret dbPassword <...>
pulumi config set --secret rabbitmqPassword <...>
pulumi config set imageTag <git-sha>     # set by CI from ${{ github.sha }}
```

### CI integration

The Track 3 `deploy` job runs:
```bash
pulumi login s3://<state-bucket>      # or Pulumi Cloud
pulumi stack select dev
pulumi config set imageTag ${{ github.sha }}
pulumi up --yes
```
prod uses a separate job gated by a GitHub `environment: prod` protection rule
(manual approval).

### Makefile targets

```makefile
infra-preview:   ## pulumi preview (dev)
	cd infra/pulumi && pulumi preview --stack dev

infra-up:        ## pulumi up (dev)
	cd infra/pulumi && pulumi up --stack dev

infra-destroy:
	cd infra/pulumi && pulumi destroy --stack dev
```

> Pulumi Go SDK needs Go locally to run `pulumi up`. Since the dev host has no Go,
> these targets are intended for **CI** (which has Go). For local previews, run
> them inside the same `golang:1.26-alpine` Podman pattern as `make build`, with
> AWS creds passed through as env vars — documented in `infra/pulumi/README.md`.

### Verification

1. `make infra-preview` → Pulumi shows the planned VPC/EKS/RDS/Amazon MQ graph.
2. `make infra-up` (dev) → cluster `ACTIVE`, `kubectl get nodes` ready.
3. `kubectl get scaledobject -n transaction-outbox` → one per method, `READY=True`.
4. CI merge to `main` → images in ECR → `deploy` job's `pulumi up` rolls out the
   new SHA → pods `Running`.
5. Load PIX traffic → only the PIX consumer Deployment scales; idle methods stay
   at 0 replicas.

---

## Track 5 — Cross-Cutting Adjustments

### Tests to add / adjust

| Area | Test |
|---|---|
| Routing (unit) | `RoutingKeyFor("PIX") == "payment.pix"`; queue/DLQ name helpers |
| Card DTO (unit) | type↔method consistency, issuer enum, PAN digit/length validation, all failure messages PII-free |
| PAN masking (unit) | `maskPAN` keeps last-4, masks the rest, handles odd lengths, non-numeric → error before masking |
| PII (unit) | `pii.Redact` masks `cardNumber` in JSON and in `key=value` text |
| Per-method routing (integration) | a PIX POST lands only in `payments.pix.queue`; a CARD POST only in its queue; cross-method isolation |
| Consumer binding (integration) | a worker started with `PAYMENT_QUEUE=payments.pix.queue` consumes PIX only, ignores BOLETO |
| Card E2E (integration) | full path for `CARTAO_CREDITO`; assert persisted `method_details` has masked PAN; DLQ on poison shows redaction |
| Unknown-method reject (integration) | method not in `Methods` → `400`, nothing published |

The Phase 2 integration suite's single-queue assumptions (`payments.queue`) must be
updated to the per-method queues. `TestMain` declares the full topology via the new
`DeclareTopology`.

### Docker Compose (local dev)

`deployments/docker-compose.yml` grows from one `consumer-worker` to **one service
per method**, each with `PAYMENT_QUEUE` set:

```yaml
consumer-pix:
  build: { context: .., dockerfile: build/Dockerfile, args: { SERVICE: consumer-worker } }
  environment: { PAYMENT_QUEUE: payments.pix.queue, ... }
consumer-boleto:
  environment: { PAYMENT_QUEUE: payments.boleto.queue, ... }
consumer-transfer:    { environment: { PAYMENT_QUEUE: payments.transfer.queue } }
consumer-cartao-credito: { environment: { PAYMENT_QUEUE: payments.cartao_credito.queue } }
consumer-cartao-debito:  { environment: { PAYMENT_QUEUE: payments.cartao_debito.queue } }
```

`make seed` gains variants (`seed-pix`, `seed-boleto`, `seed-card`) posting each
method so the per-queue routing is easy to demo.

### Config

- `PAYMENT_QUEUE` (consumer-worker, required) — added in Track 1.
- `.env.example` documents `PAYMENT_QUEUE` and the new card seed examples.

### Observability

- Consumer spans/logs already carry `message_id`; add a `payment_queue` /
  `payment_method` resource attribute on the consumer so traces and the
  `outbox.published_total` metric can be sliced per method.
- Optional: a `outbox.published_total{method=...}` metric dimension so a
  Grafana panel shows publish rate per method (feeds capacity planning for the
  per-method KEDA limits).

### Docs

- `README.md` — update the mermaid diagram to show the topic exchange fanning into
  per-method queues + per-method consumers; document the card methods and PAN
  masking; add the CI/CD + Pulumi sections.
- `CLAUDE.md` — extend "Three first-class methods" to five; document the
  per-method queue convention, `PAYMENT_QUEUE`, PAN masking at the boundary, the
  `cardNumber` PII key, and the CI gate order.

---

## Track 6 — k6 Load Tests (Latency · Autoscaling · Consumer)

**Three distinct k6 scripts**, with different purposes — the first two drive the
HTTP ingest side, the third isolates the consumer:

| Script | Drives | Consumer capacity | Measures |
|---|---|---|---|
| **6.1 `k6-baseline.js`** | HTTP `POST /payments` | **pinned at 1 pod/queue** (KEDA disabled) | P95/P99 latency at a fixed, known capacity |
| **6.2 `k6-autoscale.js`** | HTTP `POST /payments` | **KEDA active** (`min 0 / max 10`, target 1000 msgs) | autoscaling: scale-up past 1000 backlog, then scale-down to 0 |
| **6.3 `k6-consumer.js`** | **AMQP publish → queue** (xk6-amqp), observes Postgres (xk6-sql) | pinned 1 pod (clean drain rate), optionally unpinned | consumer-worker drain rate + consume→persist latency incl. the DB write |

Run separately (different setups). 6.2 **must not** pin replicas; 6.3 needs a
custom k6 binary with xk6 extensions (see that test).

### Test 6.1 — Latency Baseline (P95 / P99, pinned 1 pod/queue)

#### Goal

A reproducible load test that drives traffic at the `ingestion-api` and reports
**P95 / P99 latency** so we get a concrete read on the architecture's behaviour.
Two phases back-to-back:

1. **Phase A (5 min) — all methods, mixed.** 100 virtual users continuously POST
   `/api/v1/payments`, **round-robining across every method** (`PIX`, `BOLETO`,
   `TRANSFER`, `CARTAO_CREDITO`, `CARTAO_DEBITO`) so all five queues take load
   simultaneously.
2. **Phase B (5 min) — PIX only.** The same 100 VUs POST **only `PIX`**, so we can
   contrast a single hot queue against the spread-out mixed load.

At the end, k6 prints the **P95 and P99** of the request duration (overall and
per-method, via tags) plus throughput and error rate.

#### Decisions

- **Tool: k6** (Grafana k6) — script in JavaScript under `loadtest/`. Run via the
  official `grafana/k6` container (consistent with the Podman-based, no-host-tools
  convention): `podman run --rm -i grafana/k6 run - <script.js`.
- **Fixed consumer capacity — one pod per queue.** This test measures the
  architecture at a **known, pinned** consumer capacity, **not** KEDA's autoscaling
  behaviour. Before the run, each per-method consumer is pinned to **exactly 1
  replica**:
  - On Kubernetes: temporarily set the KEDA `ScaledObject`'s
    `minReplicaCount: 1` **and** `maxReplicaCount: 1` for every method (or suspend
    the ScaledObject with the `autoscaling.keda.sh/paused-replicas: "1"`
    annotation, which holds the deployment at 1 and stops KEDA scaling). This is
    the cloud equivalent of the requirement "one consumer pod per queue".
  - On local compose: each `consumer-<method>` service already runs a single
    instance — leave it at 1 (do not `--scale`).
  - The point: with consumers fixed at 1/queue, the P95/P99 and the queue-depth
    growth tell us the **steady-state drain rate per method** and how Phase B's
    PIX-only burst backs up a single PIX consumer. That backlog is exactly what
    would *trigger* KEDA in a non-pinned run — so this baseline also justifies the
    per-method `targetQueueLength`.
- **Load profile via k6 stages:** two 5-minute stages held at 100 VUs (a short
  ramp-up at the very start so the numbers aren't skewed by cold connections).
- **Thresholds** declared so the run gets a pass/fail signal, e.g.
  `http_req_duration: ["p(95)<...","p(99)<..."]` and `http_req_failed: ["rate<0.01"]`.
  Initial threshold values are placeholders to be calibrated after the first run
  (the first run *is* the baseline measurement).
- **Per-method breakdown:** each request is tagged `{ method: "PIX" }` etc., so
  k6's end-of-test summary and any `--out` export slice P95/P99 per method — that's
  what makes the report actionable about *which* queue/consumer is the bottleneck.
- **Idempotency-key per request:** the script sends a unique `Idempotency-Key`
  (and unique `eventId`) per iteration so every request creates a distinct outbox
  row — otherwise dedup would collapse the load and the test would measure nothing.

#### Script structure

```
loadtest/
├── k6-baseline.js        # 6.1: stages, VUs, thresholds, the two-phase scenario
├── k6-autoscale.js       # 6.2: high-rate producer to trigger KEDA (see Test 6.2)
├── k6-consumer.js        # 6.3: xk6-amqp publish → xk6-sql observe payments (see Test 6.3)
├── payloads.js           # one valid body builder per method (PIX/BOLETO/TRANSFER/CARD×2)
└── README.md             # how to run all three; pin (6.1/6.3) vs unpin (6.2) consumers
```

`k6-baseline.js` (shape):

```js
import http from "k6/http";
import { check } from "k6";
import { Trend } from "k6/metrics";
import { buildBody, METHODS } from "./payloads.js";

export const options = {
  scenarios: {
    mixed: {                              // Phase A: all methods, 5 min
      executor: "constant-vus", vus: 100, duration: "5m",
      exec: "mixed", startTime: "0s",
    },
    pixOnly: {                            // Phase B: PIX only, 5 min
      executor: "constant-vus", vus: 100, duration: "5m",
      exec: "pixOnly", startTime: "5m",
    },
  },
  thresholds: {
    "http_req_duration": ["p(95)<500", "p(99)<1000"],   // calibrate after baseline
    "http_req_failed":   ["rate<0.01"],
  },
};

const BASE = __ENV.TARGET_URL || "http://localhost:8080";

function post(method) {
  const body = buildBody(method);                       // unique eventId + Idempotency-Key inside
  const res = http.post(`${BASE}/api/v1/payments`, JSON.stringify(body),
    { headers: { "Content-Type": "application/json",
                 "Idempotency-Key": body.__idempotencyKey },
      tags: { method } });
  check(res, { "is 201": (r) => r.status === 201 });
}

export function mixed()  { post(METHODS[__ITER % METHODS.length]); }  // round-robin all 5
export function pixOnly() { post("PIX"); }
```

#### Makefile target

```makefile
loadtest:        ## 6.1 — k6 two-phase latency baseline (TARGET_URL overridable)
	podman run --rm -i -e TARGET_URL=$(TARGET_URL) -v "$(CURDIR)/loadtest:/lt" -w /lt \
		grafana/k6 run k6-baseline.js

loadtest-report: ## 6.1 — same, exporting the full default summary to JSON for archiving
	podman run --rm -i -e TARGET_URL=$(TARGET_URL) -v "$(CURDIR)/loadtest:/lt" -w /lt \
		grafana/k6 run --summary-export=summary.json k6-baseline.js
```

#### Metrics reported (both tests)

**The report is k6's full default end-of-test summary — every built-in metric, not
a hand-picked subset — plus `dropped_iterations`** (the one extra to guarantee is
present). Do **not** override `handleSummary` to filter metrics down; keep the
standard summary so all of the below always print. The four the user cares about
most (**P95, P99, requests/sec, failure %**) fall out of this set for free.

k6's standard summary already includes **all** of these:

| Metric (default summary) | What it is |
|---|---|
| `http_req_duration` | total request time — with `avg / min / med / max / p(90) / p(95) / p(99)` |
| `http_req_waiting` | **TTFB** — server processing time (the ingestion-api figure that matters most) |
| `http_req_blocked` | time waiting for a free connection — rises when the HTTP pool saturates |
| `http_req_connecting` | TCP handshake time |
| `http_req_tls_handshaking` | TLS negotiation time (0 on plain HTTP) |
| `http_req_sending` | time writing the request bytes |
| `http_req_receiving` | time reading the response bytes |
| `http_reqs` | total requests **and the rate = RPS** |
| `http_req_failed` | **failure %** (target < 1%) |
| `checks` | % of `check()` assertions passed (e.g. "is 201") |
| `iterations` / `iteration_duration` | completed iterations + per-iteration cost |
| `vus` / `vus_max` | active and peak virtual users |
| `data_sent` / `data_received` | bytes + network throughput |

Plus the one to ensure surfaces:

| Extra metric | What it is |
|---|---|
| **`dropped_iterations`** | iterations the executor **couldn't start** — load generator didn't sustain the target rate ⇒ results understated. (Built-in, but only appears with arrival-rate executors, so it's the "extra" one to confirm — **critical for 6.2**.) |

- Every `http_req_*` timing above is a k6 `Trend`, so each carries its own
  `p(95)/p(99)` in the summary — a tail spike can be attributed to server time
  (`waiting`) vs connection wait (`blocked`) vs network (`sending`/`receiving`).
- Capture it two ways: the **human-readable default summary** printed to stdout
  (archive the console output), **and** `--summary-export=summary.json` for the
  machine-readable copy. All requests carry a `method` tag so the JSON can be
  sliced per payment method to find *which* queue/consumer is the bottleneck.
- Cross-reference with the OTel/Prometheus metrics from Phase 2
  (`outbox.published_total`, `outbox.pending_count`) and the per-method
  `payment_queue` attribute (Track 5) to see queue backlog vs latency over the run.

#### Verification

1. Pin every consumer to 1 replica (annotation / compose default).
2. `make loadtest TARGET_URL=<ingestion-api url>` → runs 10 min total (5 + 5).
3. Summary shows, overall and per-method, the four required figures —
   **P95, P99, requests/sec (`http_reqs` rate), failure % (`http_req_failed`)** —
   plus the full metric table above; failure rate near 0.
4. During Phase B, RabbitMQ UI shows `payments.pix.queue` depth climbing (single
   PIX consumer draining slower than 100 VUs produce) while the other queues idle
   — quantifying the single-consumer drain ceiling per method.

### Test 6.2 — Autoscaling (KEDA scale-up to 10, scale-down to 0)

#### Goal

The mirror image of 6.1: here we **let KEDA scale** and observe it. The test
proves the per-method `ScaledObject` reacts to backlog and, crucially, that an
idle queue **scales its consumer Deployment down to 0 pods**.

#### Decisions

- **Consumers are NOT pinned** — this test runs against the real Phase 3 KEDA
  config: per method `minReplicaCount: 0`, `maxReplicaCount: 10`, RabbitMQ
  `QueueLength` trigger `value: "1000"`. Make sure no `paused-replicas` annotation
  (left over from 6.1) is present.
- **Drive one queue past the trigger.** The script targets a **single method**
  (default `PIX`, overridable via `__ENV.METHOD`) at a **high arrival rate that
  deliberately outpaces a single consumer**, so the queue backlog climbs **past
  1000 messages** and KEDA adds pods (1 pod per ~1000 backlog, capped at 10).
  - Use a k6 `ramping-arrival-rate` (or `constant-arrival-rate`) executor — an
    **arrival-rate** model (requests/sec), not VUs, because we want to control the
    *production* rate relative to the consumer's drain rate, which is what builds
    the backlog. e.g. ramp to a few thousand req/s for a few minutes.
- **Then stop producing and watch scale-down.** After the load stage, the script
  has a **quiet tail** (or simply ends) so the queue drains to empty; KEDA's
  `cooldownPeriod` then scales the Deployment **back to 0**. The test's success
  criterion is observed out-of-band via `kubectl`, not via k6 metrics (k6 only
  produces load; the scaling is the system under test).
- **Scope:** this is a Kubernetes-only test (KEDA needs K8s). It is **not** run on
  local compose. Intended target: the EKS dev stack (Track 4) or a local
  `kind`/`minikube` with KEDA installed.

#### Profile (shape)

```js
// k6-autoscale.js
export const options = {
  scenarios: {
    flood: {                                   // outpace one consumer → backlog > 1000
      executor: "ramping-arrival-rate",
      startRate: 200, timeUnit: "1s",
      preAllocatedVUs: 200, maxVUs: 1000,
      stages: [
        { target: 2000, duration: "2m" },      // ramp the produce rate up
        { target: 2000, duration: "5m" },      // hold — backlog climbs, KEDA scales toward 10
        { target: 0,    duration: "1m" },      // stop producing
      ],
    },
  },
};
const METHOD = __ENV.METHOD || "PIX";
export default function () { post(METHOD); }    // same unique eventId/Idempotency-Key per iter
// after the run, the queue drains and KEDA scales the consumer back to 0
```

#### Makefile target

```makefile
loadtest-autoscale:  ## 6.2 — flood one queue (METHOD=PIX) to trigger KEDA scale-up
	podman run --rm -i -e TARGET_URL=$(TARGET_URL) -e METHOD=$(METHOD) \
		-v "$(CURDIR)/loadtest:/lt" -w /lt grafana/k6 run k6-autoscale.js
```

#### Metrics reported

Same k6 summary as 6.1 (**P95, P99, RPS, failure %** + the full table) for the
flooded method — but here the **scaling behaviour is the result**, measured
out-of-band via `kubectl`, not from k6. Watch **`dropped_iterations`** especially:
if non-zero, the load generator didn't sustain the target rate, so the real
backlog/scaling pressure was higher than k6 shows — bump `preAllocatedVUs`/`maxVUs`
and rerun.

#### Verification

1. Start clean: target method's consumer Deployment at **0 pods**
   (`kubectl get deploy -n transaction-outbox` → `0/0`), queue empty.
2. `make loadtest-autoscale METHOD=PIX TARGET_URL=<ingestion-api url>`.
3. Watch `kubectl get pods -n transaction-outbox -w` **and**
   `kubectl get hpa,scaledobject -n transaction-outbox`:
   - As `payments.pix.queue` depth crosses ~1000, KEDA scales the PIX consumer
     **0 → 1 → … up to the cap of 10** (and **never above 10**).
   - Other methods' consumers stay at 0 (per-queue isolation).
4. When the load stage ends and the queue drains to ~0, after `cooldownPeriod`
   KEDA scales the PIX consumer **back down to 0** — the scale-to-zero we want to
   prove.
5. Confirm the 10-pod cap held even if backlog far exceeded 10 000 (no runaway).

### Test 6.3 — Consumer-Worker in Isolation (RabbitMQ → DB)

#### Goal

Tests 6.1/6.2 drive the **HTTP ingest** side. Test 6.3 isolates the **other half
of the pipeline**: the `consumer-worker`'s `RabbitMQ → ProcessMessage → Postgres`
path. We **publish straight onto a per-method queue** (bypassing `ingestion-api`
and the `DispatchOutbox` publisher) and **observe the `payments` table** to measure
how fast the consumer drains the queue and persists rows — the consumer's
standalone throughput and consume→persist latency, including the DB write.

This needs k6 to do two things it can't out of the box: **publish AMQP messages**
and **query Postgres**. Both are covered by xk6 extensions.

#### Decisions

- **Custom k6 binary via xk6** with two extensions:
  - **`xk6-amqp`** — lets the script open an AMQP connection and `publish` to the
    exchange/queue, replicating exactly what `DispatchOutbox` puts on the wire
    (JSON body = the outbox payload, `MessageId` = a unique idempotency key,
    `DeliveryMode: persistent`, routing key `payment.<method>`).
  - **`xk6-sql`** (+ the Postgres driver) — lets the script run `SELECT`/`COUNT`
    against the `payments` table to detect drain completion and read `created_at`
    for latency, and (optionally) `TRUNCATE payments` to reset between runs.
  - Build inside Go (no host Go): a small `build/k6/Dockerfile` using the
    `grafana/xk6` image to produce a `k6-ext` binary, then run it — same
    container-based convention as the rest of the repo.
- **k6's role is producer + observer, not consumer.** The real `consumer-worker`
  is the system under test; k6 publishes the load and watches the DB. The consumer
  does the `ON CONFLICT (source_message_id) DO NOTHING` insert — that DB call is
  exactly the behaviour we want to characterise under load.
- **Two scenarios:**
  - **(a) Drain throughput.** Publish a fixed batch (e.g. `N = 100_000` unique
    messages) to one queue as fast as possible, then **poll
    `SELECT count(*) FROM payments`** until it reaches `N`; elapsed time ⇒
    **messages persisted/sec** (the consumer's real drain rate, DB write included).
  - **(b) Dedup under redelivery.** Publish a batch where a fraction carries
    **duplicate `MessageId`/`source_message_id`** (simulating RabbitMQ
    redelivery); assert the final `payments` count equals the number of *distinct*
    keys, proving the `UNIQUE` constraint / `ON CONFLICT DO NOTHING` holds under
    concurrency. (No HTTP-side dedup is involved here — this is the consumer's own
    idempotency.)
- **Unique payloads:** every published message gets a fresh `paymentId` (UUIDv7)
  and `source_message_id` except where scenario (b) deliberately repeats them —
  otherwise the consumer's dedup would collapse the batch and the throughput number
  would be meaningless.
- **Consumer capacity:** run **pinned at 1 pod** for a clean per-consumer drain
  number (like 6.1); optionally rerun **unpinned** to see the same DB-write load
  drive KEDA (like 6.2), since the backlog is built directly on the queue.
- **Scope/safety:** points at a **load/test database**, never production — the
  scenarios `TRUNCATE` and bulk-insert. Guard via a required `PGDATABASE` that must
  not be the prod name.

#### Custom metrics (on top of the standard summary)

The standard k6 summary applies, plus a few `Trend`/`Counter`/`Rate` customs that
only make sense on this path:

| Custom metric | Type | Meaning |
|---|---|---|
| `amqp_publish_duration` | Trend | time to publish one message to RabbitMQ |
| `consume_to_persist_latency` | Trend | `payments.created_at − publish_ts` — RabbitMQ + consumer + DB write latency |
| `messages_persisted` | Counter | rows confirmed in `payments` (drives the rate) |
| `messages_persisted_rate` | Rate / derived | persisted/sec — the consumer's effective throughput |
| `dedup_collisions` | Counter | scenario (b): inserts that hit `ON CONFLICT` (should equal the duplicates published) |

#### Script + build structure

```
loadtest/
├── k6-consumer.js            # 6.3: xk6-amqp publish + xk6-sql poll of payments
build/k6/
└── Dockerfile                # grafana/xk6 build → k6-ext with xk6-amqp + xk6-sql(+postgres)
```

`k6-consumer.js` (shape):

```js
import amqp from "k6/x/amqp";
import sql from "k6/x/sql";
import { Trend, Counter } from "k6/metrics";

const persistLatency = new Trend("consume_to_persist_latency", true);
const persisted = new Counter("messages_persisted");

const db = sql.open("postgres", __ENV.DATABASE_URL);
amqp.start({ connection_url: __ENV.RABBITMQ_URL });

export const options = {
  scenarios: {
    drain: { executor: "shared-iterations", vus: 50, iterations: Number(__ENV.N || 100000) },
  },
};
const METHOD = __ENV.METHOD || "PIX";

export default function () {
  const body = buildOutboxPayload(METHOD);          // unique paymentId + source_message_id
  amqp.publish({
    exchange: "payments.exchange",
    routing_key: `payment.${METHOD.toLowerCase()}`,
    message_id: body.idempotencyKey,
    body: JSON.stringify(body), persistent: true,
  });
}

export function teardown() {                         // poll the DB until the batch is drained
  // SELECT count(*) FROM payments → when == N, compute persisted/sec;
  // SELECT created_at to feed persistLatency; report dedup_collisions for scenario (b)
}
```

#### Makefile targets

```makefile
k6-ext-build:        ## build the custom k6 with xk6-amqp + xk6-sql
	podman build -t k6-ext -f build/k6/Dockerfile .

loadtest-consumer:   ## 6.3 — publish N msgs to a queue, measure consumer drain + DB writes
	podman run --rm -i -e RABBITMQ_URL=$(RABBITMQ_URL) -e DATABASE_URL=$(DATABASE_URL) \
		-e METHOD=$(METHOD) -e N=$(N) -v "$(CURDIR)/loadtest:/lt" -w /lt \
		k6-ext run k6-consumer.js
```

#### Verification

1. `make k6-ext-build` → `k6-ext` image with both extensions.
2. Pin the target method's consumer to 1 pod; `TRUNCATE payments` (test DB).
3. `make loadtest-consumer METHOD=PIX N=100000` → publishes 100k, then polls the
   DB; reports **messages persisted/sec**, `consume_to_persist_latency` p95/p99,
   and the standard summary (`amqp_publish_duration`, RPS of publishes, failures).
4. Final `payments` count == 100k (scenario a) — no loss.
5. Scenario (b): publish 100k with 10% duplicate keys → `payments` count == 90k
   distinct; `dedup_collisions` == 10k — the consumer's `ON CONFLICT` dedup holds
   under load.
6. Optional unpinned rerun → the same queue backlog drives KEDA 0→N→0 while rows
   stream into `payments`.

---

## Things to keep in mind across all tracks

1. **Dependency rule holds** — the routing key is domain metadata
   (`OutboxMessage.PaymentMethod`); only `adapter/messaging` +
   `infrastructure/rabbitmq` know how it maps to AMQP. No framework imports leak
   into `domain`/`usecase`.
2. **Mask the PAN at the boundary, before storage** — the inner layers (and
   Postgres, and RabbitMQ) must only ever see the last-4. `pii.Redact` on
   `cardNumber` is defense-in-depth, not the primary control.
3. **No silently-dropped messages** — a method with no bound queue is a topic-
   exchange black hole; reject unknown methods at ingest (decision 1a).
4. **Per-method DLQs** keep poison isolation aligned with the per-method scaling.
5. **CI gates are ordered**: build → lint → unit tests → docker → deploy, with
   integration TestContainers as an optional side branch off unit-tests. A
   build/lint/unit-test failure stops everything downstream; integration never
   gates deploy.
6. **CI may use Go directly** — the host-Podman rule is for the dev machine; CI
   and Pulumi run on Go-equipped runners. Don't try to force Podman into CI.
7. **KEDA forces Kubernetes** — that's why the AWS target is EKS, not ECS. The
   per-method ScaledObjects are the cloud expression of Track 1.
8. **`make lint` still gates locally** — every code change in Phase 3 must pass
   `make lint` (Podman) with zero issues before it's considered done; the new
   GitHub `lint` job is the CI mirror of the same rule, not a replacement.
9. **Run `make swag` after the card DTO lands** so the committed OpenAPI spec stays
   in sync.
10. **Amazon MQ exposes the RabbitMQ management API** that KEDA's `rabbitmq`
    trigger (`protocol: http`) requires — keep the `RABBITMQ_MANAGEMENT_URL`
    secret pointing at it, matching the Phase 2 `scaledobject.yaml`.
11. **Two k6 tests, opposite setups** — Test 6.1 (latency baseline) **pins** every
    consumer to 1 replica (KEDA `paused-replicas: "1"` or `min=max=1`); Test 6.2
    (autoscaling) **must not** pin and runs the real KEDA config (`min 0`,
    `max 10`, target 1000 msgs). Don't leave 6.1's pause annotation on when running
    6.2. Restore the autoscaling config after 6.1.
12. **Scale-to-zero is the headline assertion of 6.2** — `minReplicaCount: 0` means
    an idle method runs **no** consumer pods. Verify the consumer scales 0→N→0
    out-of-band via `kubectl`; k6 only produces the load, KEDA is the system under
    test. The 10-pod cap (`maxReplicaCount: 10`) must hold regardless of backlog.
```
