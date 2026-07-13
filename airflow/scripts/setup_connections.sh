#!/usr/bin/env bash
# Khởi tạo môi trường Airflow cho pipeline Medallion chạy bằng dbt-databricks.
#
# Cách chạy (trong container Airflow):
#   docker compose exec airflow-scheduler bash /opt/airflow/scripts/setup_connections.sh
#
# Lưu ý kiến trúc:
#   - cosmetics_medallion_dag chạy dbt qua BashOperator.
#   - dbt-databricks kết nối Databricks QUA profiles.yml + env vars, KHÔNG qua
#     Airflow Connection (xem databricks_processing/dbt/profiles.yml).
#   - kafka_to_bronze_loader.py cũng đọc env trực tiếp và hardcode Volume inbox.
#   => Script này hiện chỉ là sanity-check env; không cần Connection hay Variable.
set -euo pipefail

echo "Sanity-check các env var mà dbt + loader đọc trực tiếp ..."
: "${DATABRICKS_HOST:?DATABRICKS_HOST phải được set trong .env}"
: "${DATABRICKS_TOKEN:?DATABRICKS_TOKEN phải được set trong .env}"
# dbt profiles.yml có fallback riêng nên optional; in ra để dễ debug.
: "${DATABRICKS_HTTP_PATH:=/sql/1.0/warehouses/71f2704e13614d48}"

echo "  DATABRICKS_HOST      = ${DATABRICKS_HOST}"
echo "  DATABRICKS_HTTP_PATH = ${DATABRICKS_HTTP_PATH}"



# ─────────────────────────────────────────────────────────────────────────────
# Connection `databricks_default` + các Variables dưới đây CHỈ được tiêu thụ bởi
# operator của `apache-airflow-providers-databricks` (DatabricksSubmitRunOperator /
# DatabricksSqlOperator / DatabricksNotebookOperator...). DAG hiện chạy BashOperator
# + dbt nên không đụng tới chúng:
#   - cosmetics_catalog / *_schema : dbt lấy catalog/schema từ profiles.yml + dbt_project.yml.
#   - bronze_volume_path           : loader hardcode VOLUME_INBOX_PATH.
#   - notebook_* / databricks_cluster_id : DAG không submit notebook theo cluster.
# Bỏ comment từng khối khi chuyển sang provider operators.
#
# --- Airflow Connection (bỏ comment nếu dùng Databricks provider operators) ---
# airflow connections delete databricks_default 2>/dev/null || true
# airflow connections add databricks_default \
#   --conn-type databricks \
#   --conn-host "${DATABRICKS_HOST}" \
#   --conn-password "${DATABRICKS_TOKEN}" \
#   --conn-extra '{"http_path": "/api/2.0"}'
#
# --- Airflow Variables (bỏ comment nếu task/macro bắt đầu đọc chúng) ---
# airflow variables set cosmetics_catalog workspace
# airflow variables set silver_schema workspace.silver_cosmetics
# airflow variables set gold_schema workspace.gold_cosmetics
# airflow variables set bronze_volume_path /Volumes/workspace/bronze_cosmetics/inbox
# airflow variables set notebook_bronze  /Workspace/databricks_processing/bronze
# airflow variables set notebook_silver  /Workspace/databricks_processing/silver
# airflow variables set notebook_gold    /Workspace/databricks_processing/gold
# airflow variables set databricks_cluster_id "${DATABRICKS_CLUSTER_ID:-0000-000000-abcdef000}"
#
# airflow connections get databricks_default
# airflow variables list
