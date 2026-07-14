# syntax=docker/dockerfile:1
# Multi-stage build cho Kafka producer (Go).

# build
FROM golang:1.26-alpine AS builder

RUN apk add --no-cache git ca-certificates

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY main.go ./

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -ldflags="-s -w" -trimpath -o /out/producer .

# runtime: distroless, nhỏ + không shell
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /out/producer /producer

ENTRYPOINT ["/producer"]
