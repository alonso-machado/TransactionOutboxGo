# CI pipelines

Two **independent** workflows, one per microservice — `ingestion-api.yml` and
`consumer-worker.yml`. They're the same shape, but kept as separate files
rather than one matrixed workflow so a change to one service never triggers,
gates, or redeploys the other: each has its own `paths:` trigger filter, its
own run history/status badge, and its own required-check configuration in
branch protection.

```
Build → golangci-lint (GATE) → Unit Tests (GATE) → Upload (ECR/Docker Hub) → Deploy (AWS)
                                        │
                                        └── Integration Tests (OPTIONAL, flag-gated,
                                            TestContainers — never blocks the pipeline)
```

Three hard gates, in order: **build → lint → unit-tests**. Any one failing
stops everything downstream for that service — `upload` and `deploy` never
run if `unit-tests` (or anything before it) is red.

`lint` checks two different things, not just Go: `golangci-lint` for the code,
then [`actionlint`](https://github.com/rhysd/actionlint) (via
`reviewdog/action-actionlint`) for the workflow YAML itself — both these
files included. A broken expression, an undefined secret reference, bad
shell syntax in a `run:` step, or a deprecated action input (this is how the
deprecated `fail_on_error` input on the actionlint step itself got caught
while writing it) fails the same gate a Go compile error would.

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
service's** image-tag config key (`imageTagIngestionApi` or
`imageTagConsumerWorker` — see Track 4's `workloads.go`), so deploying one
service never touches the other's running pods. `PULUMI_ACCESS_TOKEN` is
required for Pulumi Cloud state backend access.

A `prod` deploy is intentionally not wired here — it should be a separate job
gated by a GitHub `environment: prod` protection rule requiring manual
approval, added once the `dev` path has been exercised for real, for each
workflow independently.

## Why two files instead of one matrixed workflow

A single workflow with `strategy.matrix.service: [ingestion-api,
consumer-worker]` is still **one workflow run** — a failure in one matrix leg
shows up in the same run as the other, and a single trigger (e.g. a path
filter) would have to cover both services' paths, so an `internal/`-only
change would always run both legs even when only one binary actually changed
behavior-relevant code. Two separate files give true independence at the
cost of duplicating ~80 lines of near-identical YAML — an acceptable
trade-off for exactly two callers.
