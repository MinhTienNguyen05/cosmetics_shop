{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='_row_hash',
        file_format='delta'
    )
}}

-- Silver: clean Bronze -> anti-join bot/spam -> cast/impute/split -> filter.
-- Idempotent: merge theo _row_hash (Bronze đã dedup -> Silver tự unique).
with bronze as (
    select * from {{ ref('stg_bronze_valid') }}
),

bot_users as (
    -- Bot user: trung bình > 100 event / session
    select user_id
    from bronze
    group by user_id
    having count(*) / count(distinct user_session) > 100
),

spam_sessions as (
    -- Spam session: > 50 cart, 0 view, 0 purchase
    select user_session
    from bronze
    group by user_session
    having
        sum(case when event_type = 'cart' then 1 else 0 end) > 50
        and sum(case when event_type = 'view' then 1 else 0 end) = 0
        and sum(case when event_type = 'purchase' then 1 else 0 end) = 0
),

cleaned as (
    select
        _row_hash,
        to_timestamp(replace(event_time, ' UTC', '')) as event_time,
        event_type,
        product_id,
        category_id,
        case
            when category_code is null or category_code = '' then 'accessories.other'
            else category_code
        end as category_code,
        case
            when brand is null or brand = '' then 'unbranded'
            else lower(brand)
        end as brand,
        cast(price as float) as price,
        user_id,
        user_session
    from bronze
)

select
    _row_hash,
    event_time,
    event_type,
    product_id,
    category_id,
    split(category_code, '[.]')[0] as main_category,
    split(category_code, '[.]')[1] as sub_category,
    brand,
    price,
    user_id,
    user_session
from cleaned
left anti join bot_users on cleaned.user_id = bot_users.user_id
left anti join spam_sessions on cleaned.user_session = spam_sessions.user_session
where
    cleaned.user_id is not null
    and cleaned.product_id is not null
    and cleaned.price > 0
