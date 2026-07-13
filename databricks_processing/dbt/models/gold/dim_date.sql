{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='date_key',
        file_format='delta'
    )
}}

-- dim_date: attribute immutable, merge by date_key.
with silver as (
    select to_date(event_time) as date_key
    from {{ ref('silver_events') }}
)
select distinct
    date_key,
    year(date_key) as year,
    month(date_key) as month,
    day(date_key) as day,
    quarter(date_key) as quarter,
    dayofweek(date_key) as day_of_week,
    case when dayofweek(date_key) in (1, 7) then true else false end as is_weekend
from silver
