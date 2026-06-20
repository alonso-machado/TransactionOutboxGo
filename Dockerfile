# ── Build stage ───────────────────────────────────────────────────────────────
FROM golang:1.26-alpine AS builder

# Which cmd/* to build — passed by docker compose (ARG SERVICE=ingestion-api)
ARG SERVICE
ENV SERVICE=${SERVICE}

WORKDIR /app

# Download dependencies first (layer-cached until go.mod/go.sum change)
COPY go.mod go.sum ./
RUN go mod download

# Copy source and build a statically linked binary
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -ldflags="-s -w" -o /app/bin/service ./cmd/${SERVICE}

# ── Final stage ───────────────────────────────────────────────────────────────
FROM gcr.io/distroless/static-debian12 AS final

# Copy only the binary — no shell, no package manager, minimal attack surface
COPY --from=builder /app/bin/service /service

EXPOSE 8080

ENTRYPOINT ["/service"]
