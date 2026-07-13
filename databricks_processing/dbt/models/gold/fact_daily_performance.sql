{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['date_key', 'brand', 'main_category'],
        file_format='delta'
    )
}}

-- Grain: 1 row / (date_key, brand, main_category). Chỉ tính purchase.
with silver as (
    select * from {{ ref('silver_events') }} where event_type = 'purchase'
)
select
    to_date(event_time) as date_key,
    brand,
    main_category,
    sum(price) as total_gmv,
    count(distinct user_session) as total_orders
from silver
group by 1, 2, 3
