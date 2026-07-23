# Plan — Phase 9: Backstage developer portal (Software Catalog + API docs + TechDocs)

> **Fully implemented (as of 2026-07-23).** This is the plan that was
> executed; kept as the historical record of the decisions made, not a
> forward-looking TODO list anymore. Two real bugs were found and fixed
> only by actually running the built image, not by code review: (1) `swag`'s
> `--exclude` flag does not prune routes discovered via `--parseDependency`
> in a shared-package setup like this repo's `internal/adapter/http` —
> ingestion-api's and tickets-api's generated specs were silently
> cross-contaminated with each other's routes until switched to `--tags`
> filtering instead (see Part A); (2) Backstage's default `UrlReaderService`
> has no reader registered for the `file:` scheme at all, so the API
> entities' `definition.$text` could never resolve against a `type: file`
> catalog location — fixed by embedding the OpenAPI spec content directly
> in `catalog-info.yaml` instead of referencing it externally. A third,
> smaller fix: the guest auth provider refuses to sign in under
> `NODE_ENV=production` (which this container always runs under) unless
> `auth.providers.guest.dangerouslyAllowOutsideDevelopment: true` is set.

> Builds on Phase 7 (`.claude/plan-phase7-tickets-pivot.md`) and Phase 8
> (`.claude/plan-phase8-tickets-checkin-notifications.md`), which are the
> current source of truth for the six service binaries this catalogs.
> Phase 10 (`.claude/plan-phase10-kafka-migration.md`, RabbitMQ → Kafka) is
> intentionally sequenced **after** this phase, not alongside it — see that
> doc's Context section for why.

## Context

This monorepo has six Go service binaries but only one of them
(`ingestion-api`) has a working generated Swagger spec — `tickets-api` has
swaggo annotations on its handlers but was never wired into `make swag`'s
`swag init` invocation, and its router accepts a `swaggerEnabled` parameter
that is never actually used to register a `/swagger/*any` route. There is no
centralized place to browse either service's API surface, and no service
catalog at all. This phase fixes both: it closes the real swaggo gap
(Part A) and stands up a self-hosted Backstage instance (backstage.io) as a
Software Catalog + API docs + TechDocs portal over all six services, the
`outbox-admin` CLI, and the two cataloged APIs.

Scope decisions already made with the user, not re-litigated here:
- Self-hosted Backstage (own `backstage/` directory, own Dockerfile,
  compose service, Helm template) — not a hosted/cloud Backstage.
- Software Catalog + API docs + TechDocs (not, e.g., cost insights or
  scaffolder templates — out of scope for this phase).
- A single root-level `catalog-info.yaml`, not one per service directory —
  matches this repo's existing "centralize under one root artifact" habit
  (`docs/`, `helmcharts/`).

**Hard exit gate**: this phase must be fully implemented, tested, and
(ideally) committed on its own *before* Phase 10 starts. Phase 10 is
expected to touch messaging internals only, not the HTTP/Swagger contracts
Backstage catalogs — if that holds, Phase 10 shouldn't need to touch
anything in this phase's deliverables at all, and doing this phase first is
how that assumption gets tested.

---

## Part A — Prerequisite fix: the swaggo gap (a real bug, not just plumbing)

Today `Makefile`'s `swag` target runs a single `swag init -g
cmd/ingestion-api/main.go -o docs --parseDependency`. Change it to generate
two independent spec sets, one per API service, each into its own package
directory so their generated `docs.go` (which both currently declare
`package docs`) don't collide:

```
swag init -g cmd/ingestion-api/main.go -o docs/ingestion-api --parseDependency
swag init -g cmd/tickets-api/main.go -o docs/tickets-api --parseDependency
```

- `internal/adapter/http/tickets_router.go`: `NewTicketsRouter`'s
  `swaggerEnabled` parameter is currently dead. Wire it to register
  `r.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))`,
  the same way `router.go` does for `ingestion-api`, pointing at the new
  `docs/tickets-api` package's `SwaggerInfo`.
- `cmd/ingestion-api/main.go` / `cmd/tickets-api/main.go`: update the blank
  import of the generated docs package to the new per-service path.
- Regenerate and commit both spec sets (`docs/ingestion-api/`,
  `docs/tickets-api/`), removing the old single `docs/` output.

## Part B — Catalog metadata (`catalog-info.yaml`)

One root-level, multi-document YAML file:
- One `System` entity, `event-ticket-system`, grouping everything below.
- One `Component` per binary: `ingestion-api`, `outbox-worker`,
  `order-consumer-worker`, `fulfillment-consumer-worker`, `tickets-api`,
  `notification-retry-cron`, plus `outbox-admin` (`spec.type: tool`, no
  API — it's a CLI, never binds an HTTP port).
- `API` entities only for the two services with real HTTP surfaces —
  `ingestion-api` and `tickets-api` — each `spec.definition.$text` pointing
  at its `docs/<service>/swagger.json` from Part A. Each API-bearing
  Component gets `spec.providesApis: [<name>]`.
- `backstage.io/techdocs-ref: dir:.` annotation on every Component, for
  Part E.

## Part C — Backstage app scaffold

- New top-level `backstage/` directory via the standard
  `@backstage/create-app` scaffold (Node/TypeScript — the repo's first
  non-Go toolchain). `CLAUDE.md`'s build-commands and "What NOT to do"
  sections get a note that `backstage/` is not built/linted through
  `make build`/`make lint` (those stay Go-only, run inside
  `golang:1.26-alpine`).
- `app-config.yaml`: `catalog.locations` points at the repo's root
  `catalog-info.yaml`; `techdocs.builder: local`,
  `techdocs.publisher.type: local` (no cloud storage dependency — matches
  this repo's fully self-hosted posture everywhere else). The API docs
  plugin ships in the default scaffold already, no extra install.

## Part D — Local dev (podman-compose)

- Multi-stage `backstage/Dockerfile` (Node build stage → runtime stage),
  containerized like every other service here — no bare `npm`/`yarn` run
  against the host.
- New `backstage` service block in `docker-compose.yml`: backend on 7007,
  frontend on a port that doesn't collide with Grafana's 3000 (3001).
- New `make backstage-build` / `make backstage-up` targets, same shape as
  the existing `make build`/`make up`, still container-based.

## Part E — TechDocs content

- Root `mkdocs.yml` + `docs/index.md` (adapting `README.md`'s existing
  architecture Mermaid diagrams) so TechDocs has real content to render for
  each Component's docs tab.

## Part F — KIND / Helm

- `helmcharts/transaction-outbox/templates/backstage/` — Deployment +
  Service (Ingress optional). New precedent: neither Prometheus nor Grafana
  has a Helm template today (they're compose-only, per repo investigation),
  so this is the first "internal tool" template, not a retrofit of the
  observability stack — keep it minimal and scoped to Backstage only.

## Part G — CI

- New `.github/workflows/backstage.yml`, path-triggered on `backstage/**`
  only (same one-workflow-per-service, path-gated convention the six Go
  workflows already follow): install deps, typecheck, lint. No deploy job —
  matches every other workflow in this repo.

## Part H — Docs

- `README.md`: new "Developer Portal" section describing what Backstage
  catalogs and how to reach it locally/in KIND.
- `CLAUDE.md`: "Where things live" table gets a `backstage/` row; this
  file's own phase-doc link list gets a `plan-phase9-backstage.md` entry.

---

## Verification

- `make lint` / `make build` / `make test` green (Go side unaffected by
  this phase except Part A's router/import changes).
- `docker-compose up` (or `make up` + `make backstage-up`): confirm
  `/swagger/index.html` now works on **both** `ingestion-api` and
  `tickets-api`, and Backstage's catalog page lists all seven entities with
  both API entities showing a working "API docs" tab and TechDocs rendering.
- KIND: confirm the new Helm template deploys and Backstage is reachable
  the same way the other six services are.
- Exit-gate check before Phase 10 begins: everything above green, and
  (ideally) this phase committed as its own change.
