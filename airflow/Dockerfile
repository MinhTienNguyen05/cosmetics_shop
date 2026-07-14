# syntax=docker/dockerfile:1

# build Go producer
FROM golang:1.26-alpine AS go-builder
RUN apk add --no-cache git ca-certificates
WORKDIR /src
COPY local_streaming_engine/producer/go.mod local_streaming_engine/producer/go.sum ./
RUN go mod download
COPY local_streaming_engine/producer/main.go ./
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -ldflags="-s -w" -trimpath -o /producer .

# Airflow + provider/dep
FROM apache/airflow:2.7.1-python3.10

COPY --chown=airflow:airflow airflow/requirements.txt /requirements.txt
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.7.1/constraints-3.10.txt" \
    -r /requirements.txt

RUN python -m venv /opt/airflow/dbt_venv && \
    PIP_USER=0 /opt/airflow/dbt_venv/bin/pip install --no-cache-dir --upgrade pip && \
    PIP_USER=0 /opt/airflow/dbt_venv/bin/pip install --no-cache-dir dbt-databricks

COPY --from=go-builder --chown=airflow:airflow /producer /opt/airflow/dags/scripts/producer
