# Multi-stage build for redis-fatigue
# Builder stage
FROM golang:1.21-alpine AS builder

ARG GOPROXY=https://goproxy.cn,direct
ENV GOPROXY=${GOPROXY}
WORKDIR /src

# Optional: git for private modules; ca-certs & tzdata to copy zoneinfo if needed
RUN apk add --no-cache git ca-certificates tzdata && update-ca-certificates

# Cache go modules and vendor first
COPY go.mod go.sum ./
COPY vendor/ ./vendor/

# Copy source
COPY . .

# Build static binary for linux/amd64
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -mod=vendor -trimpath -ldflags="-s -w" -o /out/redis-fatigue ./main.go

# Runtime stage
FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata && update-ca-certificates
WORKDIR /app

# Copy binary
COPY --from=builder /out/redis-fatigue /app/redis-fatigue

# Default config (can be overridden by ConfigMap mount in K8s)
COPY config.yaml /app/config.yaml

ENV TZ=Asia/Shanghai

# Run as non-root
RUN addgroup -S app && adduser -S -G app app
RUN chown -R app:app /app
USER app

ENTRYPOINT ["/app/redis-fatigue"]
