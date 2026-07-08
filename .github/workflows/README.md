# CI pipelines

Six **independent** workflows, one per microservice — `ingestion-api.yml`,
`outbox-worker.yml`, `order-consumer-worker.yml`,
`fulfillment-consumer-worker.yml`, `tickets-api.yml`, and
`notification-consumer-worker.yml`. They're the same shape, but kept as
separate files rather than one matrixed workflow so a change to one service
never triggers or gates the others: each has its own `paths:` trigger filter
(scoped to its own `cmd/<service>/**` plus the shared
`internal/**`/`go.mod`/`go.sum`/`Dockerfile`), its own run history/status
badge, and its own required-check configuration in branch protection.

```
Build → lint: golangci-lint + actionlint + helm lint + govulncheck (GATE) → Unit Tests (GATE) → Upload (ECR/Docker Hub)
                                        │
                                        └── Integration Tests (OPTIONAL, flag-gated,
                                            TestContainers — never blocks the pipeline)
```

Three hard gates, in order: **build → lint → unit-tests**. Any one failing
stops everything downstream for that service — `upload` never runs if
`unit-tests` (or anything before it) is red.

`lint` checks four different things, not just Go: `golangci-lint` for the
code, [`actionlint`](https://github.com/rhysd/actionlint) (via
`reviewdog/action-actionlint`) for the workflow YAML itself — both these
files included — `helm lint` against `helmcharts/transaction-outbox`
(the chart deployed to Kubernetes; see the chart's own README), and
`govulncheck` (`go run golang.org/x/vuln/cmd/govulncheck@latest ./...`) —
Go's official vulnerability database, filtered to CVEs actually
**reachable** from this code's call graph rather than every vulnerable
version merely present in `go.sum`. A broken expression, an undefined
secret reference, bad shell syntax in a `run:` step, a deprecated action
input (this is how the deprecated `fail_on_error` input on the actionlint
step itself got caught while writing it), a broken chart template/values
schema, or a reachable CVE (this gate caught a real one — `go-jose/v3`
GO-2026-4945, pulled in transitively by the Clerk SDK added in Phase 8,
fixed by bumping that dependency directly) all fail the same gate a Go
compile error would.

`integration-tests` (the TestContainers suite) is a **safety measure, not a
release gate**: it needs `unit-tests` to reuse the gate result, but nothing
downstream needs it, so it can fail or be skipped without blocking `upload`.
It's off by default and only runs when explicitly requested:

- via `workflow_dispatch` with the `run_integration` input checked, or
- on a pull request labeled `ci:integration`.

`upload` only runs on pushes to `main`, after the gates pass. It builds that
service's image (root `Dockerfile`, `ARG SERVICE=...`) and pushes it to ECR
(primary, OIDC-authenticated via `AWS_DEPLOY_ROLE_ARN`/`AWS_REGION`), with an
optional Docker Hub mirror push if the `DOCKERHUB_USERNAME` repo/environment
variable and `DOCKERHUB_TOKEN` secret are configured — if not, that step is
skipped automatically.

There is intentionally no automated `deploy` job right now — Pulumi (which
used to run `pulumi up` here) was removed from the project; Helm + KIND is
the deploy/test path instead, applied manually (`make k8s-apply`). A CI-driven
deploy step (Pulumi again, or a direct `helm upgrade`, ideally gated by a
GitHub `environment:` protection rule for anything beyond a dev cluster) can
be added back to each workflow independently once that's wired up.

## Why separate files instead of one matrixed workflow

A single workflow with `strategy.matrix.service: [ingestion-api,
outbox-worker, order-consumer-worker, fulfillment-consumer-worker,
tickets-api, notification-consumer-worker]` is still **one workflow run** —
a failure in one matrix leg shows up in the same run as the others, and a
single trigger (e.g. a path filter) would have to cover every service's
paths, so an `internal/`-only change would always run all legs even when
only one binary actually changed behavior-relevant code. Separate files
give true independence at the cost of duplicating ~80 lines of
near-identical YAML per service — an acceptable trade-off at this count.
