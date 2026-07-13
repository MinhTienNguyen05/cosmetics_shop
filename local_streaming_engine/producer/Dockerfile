# syntax=docker/dockerfile:1
# Multi-stage build cho Kafka producer (Go).
# Build binary linux/amd64 bên trong Docker — KHÔNG commit binary vào repo.

# ---- Stage 1: build ----
FROM golang:1.26-alpine AS builder

# Tạo user không-privilege để distroless chạy được (uid phải tồn tại ở runtime stage).
RUN apk add --no-cache git ca-certificates

WORKDIR /src

# Cache layer dependency: chỉ copy go.mod/go.sum trước.
COPY go.mod go.sum ./
RUN go mod download

COPY main.go ./

# Static binary, strip debug info, tắt CGO → chạy được trên distroless static.
# GOARCH=amd64 để khớp với image Airflow (linux/amd64).
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -ldflags="-s -w" -trimpath -o /out/producer .

# ---- Stage 2: runtime (distroless, nhỏ + không shell → an toàn) ----
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /out/producer /producer

# Producer đọc KAFKA_BROKERS và data path từ env khi chạy.
ENTRYPOINT ["/producer"]
