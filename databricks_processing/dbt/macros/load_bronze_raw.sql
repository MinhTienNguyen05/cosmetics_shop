{% macro load_bronze_raw() %}
  {# Land JSON từ Volume inbox vào bảng raw bằng COPY INTO (chạy trên SQL warehouse).
     COPY INTO idempotent: track file đã load trong Delta log -> re-run chỉ load file mới.
     Chạy bằng:  dbt run-operation load_bronze_raw

     LƯU Ý 1: raw SQL viết trực tiếp trong macro KHÔNG tự execute khi gọi qua
     `dbt run-operation` (Jinja chỉ render text). Phải gói trong run_query()
     thì dbt mới thực sự gửi statement xuống warehouse.

     LƯU Ý 2 (batch-safe): COPY INTO raise COPY_INTO_SOURCE_SCHEMA_INFERENCE_FAILED
     khi inbox trống (đợt rỗng / lần đầu). SQL Warehouse không cho SET
     spark.databricks.delta.copyInto.emptySourceCheck.enabled (Spark config),
     nên check LIST inbox trước: có file -> COPY INTO, không -> no-op. #}
  {% do run_query("
    CREATE TABLE IF NOT EXISTS workspace.bronze_cosmetics.cosmetics_events_bronze_raw (
      event_time    STRING,
      event_type    STRING,
      product_id    STRING,
      category_id   STRING,
      category_code STRING,
      brand         STRING,
      price         STRING,
      user_id       STRING,
      user_session  STRING
    ) USING DELTA
  ") %}

  {% set listing = run_query("LIST '/Volumes/workspace/bronze_cosmetics/inbox'") %}
  {% if listing is not none and (listing.rows | length) > 0 %}
    {% do run_query("
      COPY INTO workspace.bronze_cosmetics.cosmetics_events_bronze_raw
      FROM '/Volumes/workspace/bronze_cosmetics/inbox'
      FILEFORMAT = JSON
    ") %}
    {{ log("load_bronze_raw: COPY INTO đã chạy (inbox có " ~ (listing.rows | length) ~ " file).", info=True) }}
  {% else %}
    {{ log("load_bronze_raw: inbox trống — skip COPY INTO (no-op cho đợt rỗng/lần đầu).", info=True) }}
  {% endif %}
{% endmacro %}
