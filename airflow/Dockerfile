# syntax=docker/dockerfile:1

# ---- Stage 1: build Go producer (linux/amd64) ----
FROM golang:1.26-alpine AS go-builder
RUN apk add --no-cache git ca-certificates
WORKDIR /src
COPY local_streaming_engine/producer/go.mod local_streaming_engine/producer/go.sum ./
RUN go mod download
COPY local_streaming_engine/producer/main.go ./
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -ldflags="-s -w" -trimpath -o /producer .

# ---- Stage 2: Airflow + provider/dep ----
# apache/airflow mặc định USER=airflow; image REFUSE chạy pip as root -> KHÔNG switch root.
FROM apache/airflow:2.7.1-python3.10

COPY --chown=airflow:airflow airflow/requirements.txt /requirements.txt
# Constraints file tương ứng Airflow 2.7.1 + Python 3.10 để pip resolve an toàn.
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.7.1/constraints-3.10.txt" \
    -r /requirements.txt

# dbt-databricks trong venv RIÊNG (tránh conflict dep giữa dbt-core và Airflow).
# PIP_USER=0: image apache/airflow set PIP_USER=1 (install vào user site) -> phá venv pip,
# nên tắt cho venv. DAG gọi /opt/airflow/dbt_venv/bin/dbt.
RUN python -m venv /opt/airflow/dbt_venv && \
    PIP_USER=0 /opt/airflow/dbt_venv/bin/pip install --no-cache-dir --upgrade pip && \
    PIP_USER=0 /opt/airflow/dbt_venv/bin/pip install --no-cache-dir dbt-databricks

# Copy producer binary (executable bit được COPY giữ nguyên) vào path DAG mong đợi.
COPY --from=go-builder --chown=airflow:airflow /producer /opt/airflow/dags/scripts/producer
