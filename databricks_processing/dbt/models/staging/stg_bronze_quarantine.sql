{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='_row_hash',
        file_format='delta'
    )
}}

-- Quarantine: dòng bị loại ở Bronze (NULL key, header CSV, price non-numeric).
-- Bằng chứng NULL guard hoạt động; dùng debug/audit.
with raw as (
    select * from {{ source('bronze', 'cosmetics_events_bronze_raw') }}
),

tagged as (
    select
        raw.*,
        {{ surrogate_key([
            'event_time','event_type','product_id','category_id',
            'category_code','brand','price','user_id','user_session'
        ]) }} as _row_hash,
        cast(price as double) as _price_d
    from raw
),

bad_rows as (
    select
        _row_hash,
        event_time,
        event_type,
        product_id,
        category_id,
        category_code,
        brand,
        price,
        user_id,
        user_session
    from tagged
    where
        user_id is null or user_id = ''
        or product_id is null or product_id = ''
        or product_id = 'product_id'
        or _price_d is null
)

-- Dedup THEO _row_hash (giống stg_bronze_valid): cùng dòng bad có thể lặp trong
-- raw -> qualify để _row_hash unique, khớp test unique.
select * from bad_rows
qualify row_number() over (partition by _row_hash order by event_time) = 1
