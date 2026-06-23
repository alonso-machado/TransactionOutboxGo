# Builds a custom k6 binary with the xk6-amqp extension loadtest/k6-consumer.js
# needs — standard k6 doesn't have it. Same container-based convention as
# the rest of this repo (no host tooling).
#
# Usage: podman build -t k6-ext -f build/k6/Dockerfile .
FROM grafana/xk6:latest AS builder

# The image runs as the non-root "xk6" user (uid 1000) with a read-only /,
# so --output must land somewhere that user can write — its own WORKDIR
# (/xk6), not the root-owned / the original --output /k6 pointed at.
#
# v1.8.0 is pinned explicitly — xk6-amqp depends on the unsuffixed
# go.k6.io/k6 (v1) module path, and without a pin xk6 auto-resolves to the
# newest k6 (v2), which silently drops the amqp module.
RUN xk6 build v1.8.0 \
    --with github.com/grafana/xk6-amqp@latest \
    --output /xk6/k6

FROM grafana/k6:latest
COPY --from=builder /xk6/k6 /usr/bin/k6
ENTRYPOINT ["k6"]
