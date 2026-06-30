# CI pipelines

Three **independent** workflows, one per microservice — `ingestion-api.yml`,
`outbox-worker.yml`, and `consumer-worker.yml`. They're the same shape, but
kept as separate files rather than one matrixed workflow so a change to one
service never triggers, gates, or redeploys the others: each has its own
`paths:` trigger filter (scoped to its own `cmd/<service>/**` plus the shared
`internal/**`/`go.mod`/`go.sum`/`Dockerfile`), its own run history/status
badge, and its own required-check configuration in branch protection.

```
Build → golangci-lint (GATE) → Unit Tests (GATE) → Upload (ECR/Docker Hub) → Deploy (AWS)
                                        │
                                        └── Integration Tests (OPTIONAL, flag-gated,
                                            TestContainers — never blocks the pipeline)
```

Three hard gates, in order: **build → lint → unit-tests**. Any one failing
stops everything downstream for that service — `upload` and `deploy` never
run if `unit-tests` (or anything before it) is red.

`lint` checks three different things, not just Go: `golangci-lint` for the
code, [`actionlint`](https://github.com/rhysd/actionlint) (via
`reviewdog/action-actionlint`) for the workflow YAML itself — both these
files included — and `helm lint` against `helmcharts/transaction-outbox`
(the chart the `deploy` job installs, via Track 4's Pulumi program). A
broken expression, an undefined secret reference, bad shell syntax in a
`run:` step, a deprecated action input (this is how the deprecated
`fail_on_error` input on the actionlint step itself got caught while writing
it), or a broken chart template/values schema all fail the same gate a Go
compile error would.

`integration-tests` (the TestContainers suite) is a **safety measure, not a
release gate**: it needs `unit-tests` to reuse the gate result, but nothing
downstream needs it, so it can fail or be skipped without blocking
`upload`/`deploy`. It's off by default and only runs when explicitly
requested:

- via `workflow_dispatch` with the `run_integration` input checked, or
- on a pull request labeled `ci:integration`.

`upload` and `deploy` only run on pushes to `main`, after the gates pass.
`upload` builds that service's image (root `Dockerfile`, `ARG SERVICE=...`)
and pushes it to ECR (primary, OIDC-authenticated via `AWS_DEPLOY_ROLE_ARN`/
`AWS_REGION`), with an optional Docker Hub mirror push if the
`DOCKERHUB_USERNAME` repo/environment variable and `DOCKERHUB_TOKEN` secret
are configured — if not, that step is skipped automatically. `deploy` runs
`pulumi up` against the `dev` stack in `infra/pulumi/`, setting **only that
service's** image-tag config key (`imageTagIngestionApi`,
`imageTagOutboxWorker`, or `imageTagConsumerWorker` — see `config.go`/
`workloads.go`), so deploying one service never touches the others' running
pods. `PULUMI_ACCESS_TOKEN` is required for Pulumi Cloud state backend access.

A `prod` deploy is intentionally not wired here — it should be a separate job
gated by a GitHub `environment: prod` protection rule requiring manual
approval, added once the `dev` path has been exercised for real, for each
workflow independently.

## Why separate files instead of one matrixed workflow

A single workflow with `strategy.matrix.service: [ingestion-api,
outbox-worker, consumer-worker]` is still **one workflow run** — a failure in
one matrix leg shows up in the same run as the others, and a single trigger
(e.g. a path filter) would have to cover every service's paths, so an
`internal/`-only change would always run all legs even when only one binary
actually changed behavior-relevant code. Separate files give true
independence at the cost of duplicating ~80 lines of near-identical YAML per
service — an acceptable trade-off at this count.
