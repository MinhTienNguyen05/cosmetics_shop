{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='_row_hash',
        file_format='delta'
    )
}}

-- Clean Bronze: dedup theo _row_hash (merge insert/update), chỉ giữ row HỢP LỆ.
-- NULL/header/non-numeric price đi sang stg_bronze_quarantine.
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

valid_rows as (
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
        user_id is not null and user_id <> ''
        and product_id is not null and product_id <> ''
        and product_id <> 'product_id'   -- dòng header CSV
        and _price_d is not null         -- price không cast được sang số
)

-- Dedup THEO _row_hash: raw (CSV thật) có dòng trùng lặp hoàn toàn (cùng 9 field
-- -> cùng md5). Merge chỉ dedup GIỮA các batch, không dedup TRONG batch; và
-- full-refresh = CREATE TABLE AS (không dedup). Nên phải qualify row_number()
-- để _row_hash thực sự unique — khớp test unique + grain 1 row/_row_hash.
select * from valid_rows
qualify row_number() over (partition by _row_hash order by event_time) = 1
