{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='product_id',
        file_format='delta'
    )
}}

-- dim_product (SCD1): 1 row/product, surrogate product_key=md5(product_id).
with silver as (
    select * from {{ ref('silver_events') }}
),
ranked as (
    select
        product_id,
        main_category,
        sub_category,
        brand,
        row_number() over (
            partition by product_id
            order by main_category, sub_category, brand
        ) as rn
    from silver
)
select
    {{ surrogate_key(['product_id']) }} as product_key,
    product_id,
    main_category,
    sub_category,
    brand
from ranked
where rn = 1
