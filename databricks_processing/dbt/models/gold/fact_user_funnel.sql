{{
  config(
    materialized = 'incremental',
    unique_key = ['date_key', 'user_session', 'user_id', 'product_id'],
    incremental_strategy = 'merge'
  )
}}

WITH raw_events AS (
    SELECT
        CAST(event_time AS DATE) AS date_key,
        user_session,
        user_id,
        product_id,
        event_type
    FROM {{ ref('silver_events') }}
    WHERE event_type IN ('view', 'cart', 'purchase')

    {% if is_incremental() %}
      AND CAST(event_time AS DATE) >= coalesce((SELECT MAX(date_key) FROM {{ this }}), '1970-01-01')
    {% endif %}
)

SELECT
    date_key,
    user_session,
    user_id,
    product_id,
    -- Thêm cột product_key để khớp với schema hiện tại của bảng đích
    md5(cast(product_id as string)) AS product_key,
    SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) AS total_views,
    SUM(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) AS total_add_to_carts,
    SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS total_purchases
FROM raw_events
GROUP BY
    date_key,
    user_session,
    user_id,
    product_id