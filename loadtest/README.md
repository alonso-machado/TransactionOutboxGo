# Load tests (Phase 3 Track 6)

## KIND environment setup (for 6.2, and 6.1/6.3 against a real cluster)

6.2 needs a real KEDA `ScaledObject`, which only Kubernetes provides — a
local [KIND](https://kind.sigs.k8s.io/) cluster is the cheapest way to get
one without touching AWS. Infra (Postgres/RabbitMQ) lives in the
`default` namespace; the app (ingestion-api/order-consumer-worker/
fulfillment-consumer-worker) is the Helm release in its own
`transaction-outbox` namespace — same split the chart and
`infra/kind/*.yaml` assume everywhere else in this repo.

```bash
# 1. Create the cluster (skip if `kind get clusters` already shows one).
#    infra/kind/kind-cluster-config.yaml maps host 15673 -> the RabbitMQ
#    management NodePort set up in step 6 below — recreate the cluster with
#    this config if you already have one without that port mapping.
kind create cluster --name kind-cluster --config infra/kind/kind-cluster-config.yaml

# 2. KEDA — the chart's ScaledObject CRDs need the KEDA operator installed
helm repo add kedacore https://kedacore.github.io/charts
helm repo update
helm install keda kedacore/keda --namespace keda --create-namespace

# 3. metrics-server — ingestionApi.hpa (templates/ingestion-api/hpa.yaml)
#    targets CPU/memory utilization, which is a no-op without this: `kubectl
#    top` and the HPA's metrics both come from here, and KIND doesn't ship
#    it by default. --kubelet-insecure-tls is required because KIND's
#    kubelet serving certs aren't signed for metrics-server's normal
#    verification path — fine for a local throwaway cluster, never do this
#    on a real one. Same pattern as KEDA above: cluster-level infra,
#    installed once, NOT part of the transaction-outbox chart, namespaced
#    to "default" here (its upstream manifest defaults to kube-system) to
#    match this repo's KIND-infra convention.
curl -sL -o /tmp/metrics-server.yaml \
  https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml
sed -i 's/namespace: kube-system/namespace: default/g' /tmp/metrics-server.yaml
sed -i '/--kubelet-use-node-status-port/a\        - --kubelet-insecure-tls' /tmp/metrics-server.yaml
# The auth-reader RoleBinding must stay in kube-system — it binds to the
# "extension-apiserver-authentication-reader" Role, which only exists
# there (RoleBindings can't reference a Role outside their own namespace).
# Its ServiceAccount *subject* staying "default" is fine — that's a
# cross-namespace reference, which RoleBindings do support.
sed -i '/name: metrics-server-auth-reader/{n;s/namespace: default/namespace: kube-system/}' /tmp/metrics-server.yaml
kubectl apply -f /tmp/metrics-server.yaml
kubectl wait --for=condition=available deployment/metrics-server -n default --timeout=60s

# 4. Infra (Postgres + RabbitMQ) — plain Deployments in "default", not the
#    chart's normal RDS/Amazon MQ targets (see infra/kind/postgres-rabbitmq.yaml)
kubectl apply -f infra/kind/postgres-rabbitmq.yaml
kubectl wait --for=condition=ready pod -l app=postgres --timeout=60s
kubectl wait --for=condition=ready pod -l app=rabbitmq --timeout=60s

# 5. Migrations — one-shot Job sourced from ConfigMaps (KIND has no host-path
#    mount for this cluster, unlike compose's volume mount). Two-DB split: one
#    ConfigMap per migration set (the sets have overlapping version numbers and
#    golang-migrate tracks schema_migrations per database, so they can't share
#    a path). The Job also CREATE-DATABASEs `events` first — see
#    infra/kind/migrate-job.yaml.
kubectl create configmap migrations-outbox --from-file=migrations/outbox/
kubectl create configmap migrations-events --from-file=migrations/events/
kubectl apply -f infra/kind/migrate-job.yaml
kubectl wait --for=condition=complete job/migrate --timeout=60s

# 6. Build the app images and load them straight into KIND's node (no
#    registry push needed — `:kindtest` matches infra/kind/values-kind.yaml)
podman build --build-arg SERVICE=ingestion-api               -t localhost/transaction-outbox-go/ingestion-api:kindtest               .
podman build --build-arg SERVICE=outbox-worker                -t localhost/transaction-outbox-go/outbox-worker:kindtest               .
podman build --build-arg SERVICE=order-consumer-worker        -t localhost/transaction-outbox-go/order-consumer-worker:kindtest       .
podman build --build-arg SERVICE=fulfillment-consumer-worker  -t localhost/transaction-outbox-go/fulfillment-consumer-worker:kindtest .
podman save localhost/transaction-outbox-go/ingestion-api:kindtest               -o /tmp/ingestion-api.tar
podman save localhost/transaction-outbox-go/outbox-worker:kindtest               -o /tmp/outbox-worker.tar
podman save localhost/transaction-outbox-go/order-consumer-worker:kindtest       -o /tmp/order-consumer-worker.tar
podman save localhost/transaction-outbox-go/fulfillment-consumer-worker:kindtest -o /tmp/fulfillment-consumer-worker.tar
kind load image-archive /tmp/ingestion-api.tar               --name kind-cluster
kind load image-archive /tmp/outbox-worker.tar               --name kind-cluster
kind load image-archive /tmp/order-consumer-worker.tar       --name kind-cluster
kind load image-archive /tmp/fulfillment-consumer-worker.tar --name kind-cluster

# 7. Install the chart with the KIND overrides (KEDA-driven autoscaling,
#    in-cluster DB/MQ endpoints, rate limiter off, kindLocal.enabled=true
#    which renders templates/kind-local/rabbitmq-nodeport.yaml — see
#    infra/kind/values-kind.yaml's comments)
helm upgrade --install transaction-outbox helmcharts/transaction-outbox \
  --namespace transaction-outbox --create-namespace \
  -f infra/kind/values-kind.yaml
```

Re-run step 6 + `helm upgrade --install ...` (step 7) whenever app code
changes — `kind load image-archive` replaces the node's cached image, but
existing pods keep running the old one until rolled, so also run:

```bash
kubectl rollout restart deployment -n transaction-outbox
```

### Reaching things from the host

RabbitMQ's management UI/API is reachable straight at `localhost:15673` —
no `port-forward` needed — once steps 1 and 6 above are both in place:
`kind-cluster-config.yaml`'s `extraPortMappings` map host 15673 to the
control-plane node's 30673, and `kindLocal.enabled: true` in
`values-kind.yaml` makes the chart render a NodePort Service
(`templates/kind-local/rabbitmq-nodeport.yaml`) publishing RabbitMQ's
15672 on that same 30673. `loadtest-consumer`'s
`curl -u guest:guest http://localhost:15672/...` examples below become
`http://localhost:15673/...` against the KIND cluster's RabbitMQ instead of
compose's (which keeps the plain 15672).

`ingestion-api` gets the same treatment for 6.1/6.2's `TARGET_URL` — `k6`
runs on the Windows host, so a fixed NodePort beats keeping a
`kubectl port-forward` alive in another terminal for the whole test run.
`kindLocal.enabled: true` forces `templates/ingestion-api/service.yaml` to
`type: NodePort` on a fixed `nodePort` (`kindLocal.ingestionApiNodePort`,
default `30080`), and `kind-cluster-config.yaml` maps that to host `8081`
(not `8080`, so it doesn't clash with compose's own host-published
ingestion-api):

```bash
make loadtest-autoscale EVENT_TYPE=CONCERT EVENT_SUBTYPE=ROCK TARGET_URL=http://localhost:8081
```

Only Postgres has no host-reachable port — nothing in this load-test suite
needs to reach it directly; `kubectl port-forward -n default svc/postgres
5432:5432` works the same way as RabbitMQ's did before it got a NodePort, if
you ever need to.

Three k6 scripts, run separately with **opposite consumer setups** — don't
mix them up:

| Script | Drives | Consumer capacity | Measures |
|---|---|---|---|
| `k6-baseline.js` | HTTP `POST /orders` | **pinned at 1 pod/shard** (KEDA disabled/paused) | P95/P99 latency at a fixed, known capacity |
| `k6-autoscale.js` | HTTP `POST /orders` | **KEDA active** (`min 0 / max 10`, target 1000 msgs) | autoscaling: scale-up past 1000 backlog, then scale-down to 0 |
| `k6-consumer.js` | AMQP publish straight onto an order queue (bypasses ingestion-api) | pinned 1 pod for a clean number, or unpinned | queue backlog/drain rate (RabbitMQ side) + order-consumer-worker's outcome breakdown (`ack`/`duplicate`/`unknown_schema_version`/...) via its `/metrics`, **DB write included** but checked separately, not by k6 |

## 6.1 — Latency baseline

```bash
make loadtest-up
make loadtest TARGET_URL=http://localhost:8080
make loadtest-report TARGET_URL=http://localhost:8080   # + summary-baseline.json
```

Two 5-minute phases at a fixed 100 virtual users: Phase A round-robins across
`SHARDS` (`payloads.js` — CONCERT/ROCK and SPORTS/FOOTBALL by default), Phase
B hits CONCERT/ROCK only. **Pin every consumer to 1 replica first** — both
compose files already run a single instance per shard (leave it); on
Kubernetes, set the KEDA `ScaledObject`'s `minReplicaCount`/`maxReplicaCount`
to 1, or annotate it `autoscaling.keda.sh/paused-replicas: "1"`.

Reports the full k6 default summary (P95/P99, RPS, failure rate, the works) —
every request is tagged `{ eventType, eventSubtype }` so the summary/JSON
export can be sliced per shard to find the bottleneck. Watch
`dropped_iterations`: if non-zero, the load generator didn't sustain the
target rate.

> **Results section pending a fresh run.** The result numbers this section
> used to carry (P95/P99, throughput, backlog counts) were captured against
> the pre-pivot payments domain (`POST /api/v1/payments`, per-payment-method
> queues) — the architecture and bottleneck shape (`DispatchOutbox`'s
> batch/interval tuning being the real ceiling, not the HTTP/DB write path)
> should carry over unchanged, since order intake follows the identical
> outbox pattern, but the exact figures need re-measuring against
> `POST /api/v1/orders` before being trusted for this domain. Re-run 6.1 and
> replace this note with fresh numbers.

## 6.2 — Autoscaling (Kubernetes-only)

```bash
make loadtest-autoscale EVENT_TYPE=CONCERT EVENT_SUBTYPE=ROCK TARGET_URL=<ingestion-api-url>
```

Floods one shard (default CONCERT/ROCK) at a rate that outpaces its single
order-consumer-worker instance, driving that shard's order queue backlog
past the KEDA trigger (`queueLengthValue: "1000"`, see
`helmcharts/transaction-outbox/values.yaml`). **Do not pin consumers for
this test** — remove any `paused-replicas` annotation left over from 6.1
first. Watch `kubectl get pods,scaledobject -n transaction-outbox`: the
target shard's `order-consumer-worker-<name>` should scale 0→1→…→10 (and
never above), then back to 0 once the load stage ends and the queue drains
(after `cooldownPeriod`). The scaling is the system under test, observed
out-of-band — k6 only produces the load.

## 6.3 — order-consumer-worker in isolation

Needs a custom k6 binary with the `xk6-amqp` extension (`build/k6/Dockerfile`).
k6 only does the publish side here — **order-consumer-worker's own behavior
is read from its Prometheus `/metrics` endpoint**, not from k6 — see
"Checking consumer behavior" below. Scoped to the order stream only, not
fulfillment-consumer-worker — see `k6-consumer.js`'s header comment for why.

```bash
make loadtest-up
make k6-ext-build
make loadtest-consumer SHARDS=CONCERT:ROCK N=50000 \
  RABBITMQ_URL=amqp://guest:guest@localhost:5672/
```

Publishes `N` messages at a **fixed 100 VUs** straight onto
`events.<eventtype>.<eventsubtype>.queue` (the exact shape `DispatchOutbox`'s
publisher puts on the wire for `order_outbox` — bypassing ingestion-api
entirely), hitting RabbitMQ as hard as it can. Every run is a **mix** of
three message shapes by default, so a single invocation exercises all of the
consumer's outcomes in one pass:

- the rest — unique, well-formed → `outcome=ack`
- `DUP_FRACTION` (10%) — reuses a prior iteration's identity → `outcome=duplicate`
- `SCHEMA_FRACTION` (2%) — unrecognized `schemaVersion` → `outcome=unknown_schema_version`
  (rejected straight to the DLQ on the first attempt, no retry wait)

Set either fraction to `0` (`-e DUP_FRACTION=0`) for a clean-only run.

### Multiple shards at once, multiple consumers per shard

`SHARDS` (comma-separated `eventType:eventSubtype` pairs, e.g.
`CONCERT:ROCK,SPORTS:FOOTBALL`) splits `N` evenly across shards via
round-robin — each shard still writes through the same `orders`/`tickets`
tables (scoped by `event_type`/`event_subtype`, not separate tables per
shard, unlike the old per-method hypertables), so this is also how you
compare two shards' consumer+DB throughput side by side. Bring up a second
instance bound to the same queue to test whether adding consumers actually
scales DB write throughput, or whether Postgres/PgBouncer becomes the
bottleneck first — add a second `order-consumer-worker-concert-rock`-style
service to `docker-compose.yml` bound to the same `CONSUMER_QUEUE` (mirrors
the old `consumer-pix-extra` pattern) and:

```bash
make loadtest-up
make k6-ext-build
make loadtest-consumer SHARDS=CONCERT:ROCK,SPORTS:FOOTBALL N=150000 \
  RABBITMQ_URL=amqp://guest:guest@localhost:5672/
```

### Checking consumer behavior

```bash
# Snapshot before (e.g. right after `podman restart` to zero counters),
# then again after the run finishes draining, and diff. One curl per
# consumer instance — see docker-compose.yml's METRICS_PORTs for
# order-consumer-worker-concert-rock/-sports-football.
curl -s http://localhost:9091/metrics | grep consumer_messages_processed_total
curl -s http://localhost:9092/metrics | grep consumer_messages_processed_total
curl -s -u guest:guest http://localhost:15672/api/queues/%2F/events.concert.rock.queue | grep -o '"messages":[0-9]*'
curl -s -u guest:guest http://localhost:15672/api/queues/%2F/events.concert.rock.dlq | grep -o '"messages":[0-9]*'
```

`consumer_messages_processed_total{outcome=...}` is the per-outcome counter
(`ack`/`duplicate`/`retry_scheduled`/`poison_dlq`/`unknown_schema_version`);
summed across every instance bound to a shard's queue, the delta should
always equal that shard's published count exactly — no message is ever
silently lost. `make purge-loadtest-dlq STREAM=order EVENT_TYPE=CONCERT
EVENT_SUBTYPE=ROCK` removes only the `customerName="LOADTEST"` messages a
run leaves in the DLQ afterward, without touching any real message — see
the main [README.md](../README.md)'s `outbox-admin` section.

> **Results section pending a fresh run.** This section previously carried a
> worked example (2 consumers vs. 1, near-linear drain-rate scaling,
> exact ack/duplicate/unknown_schema_version reconciliation) measured
> against the payments domain's `PIX`/`TRANSFER` methods. The methodology
> and the finding (drain rate scales close to linearly with consumer count
> before Postgres/PgBouncer becomes the bottleneck) should still hold — the
> consumer/DB write path is structurally the same — but needs re-measuring
> against `CONCERT:ROCK`/`SPORTS:FOOTBALL` before citing exact numbers here
> again.
>
> One thing that DOES still apply as documented: a real, reproducible bug
> surfaced building the original version of this test — `xk6-amqp`'s
> connection registry is an unsynchronized map, and every VU calling
> `amqp.start()` concurrently at VU-init could hit a `fatal error: concurrent
> map writes` that crashed the whole process. Fixed (and still fixed in the
> current `k6-consumer.js`) by starting the connection once in `setup()`
> (runs single-threaded before any VU starts) and sharing it via
> `connection_id`.

Pin the target shard's consumer count for a clean, repeatable number; an
unpinned rerun on Kubernetes lets the same backlog drive KEDA 0→N→0,
mirroring 6.2 from the queue side.

## Shared

`payloads.js` builds a valid order body for each shard in `SHARDS`
(CONCERT/ROCK and SPORTS/FOOTBALL by default) with a unique
`sourceOrderId`/`Idempotency-Key` per call — without that, dedup collapses
every iteration into one outbox row and the test measures nothing.
