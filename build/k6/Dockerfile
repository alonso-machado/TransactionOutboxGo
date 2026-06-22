# Builds a custom k6 binary with the xk6-amqp + xk6-sql (Postgres) extensions
# loadtest/k6-consumer.js needs — standard k6 has neither. Same
# container-based convention as the rest of this repo (no host tooling).
#
# Usage: podman build -t k6-ext -f build/k6/Dockerfile .
FROM grafana/xk6:latest AS builder

RUN xk6 build \
    --with github.com/grafana/xk6-sql@latest \
    --with github.com/grafana/xk6-sql-driver-postgres@latest \
    --with github.com/grafana/xk6-amqp@latest \
    --output /k6

FROM grafana/k6:latest
COPY --from=builder /k6 /usr/bin/k6
ENTRYPOINT ["k6"]
