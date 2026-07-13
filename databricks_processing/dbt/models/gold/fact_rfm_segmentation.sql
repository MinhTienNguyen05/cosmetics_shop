{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['user_id', 'calc_date'],
        file_format='delta'
    )
}}

-- Grain: 1 row / (user_id, calc_date). RFM + segment. FK user_id -> dim_user_snapshot.
with silver as (
    select * from {{ ref('silver_events') }} where event_type = 'purchase'
),
anchor as (
    select max(to_date(event_time)) as calc_date from silver
),
agg as (
    select
        user_id,
        max(to_date(event_time)) as last_purchase,
        count(distinct user_session) as frequency,
        sum(price) as monetary
    from silver
    group by user_id
)
select
    a.user_id,
    an.calc_date,
    datediff(an.calc_date, a.last_purchase) as recency,
    a.frequency,
    a.monetary,
    case
        when datediff(an.calc_date, a.last_purchase) <= 30
             and a.frequency >= 3 and a.monetary > 50 then 'VIP'
        when datediff(an.calc_date, a.last_purchase) <= 30
             and a.frequency = 1 then 'Newbie'
        when datediff(an.calc_date, a.last_purchase) > 60 then 'Churning'
        else 'Regular'
    end as customer_segment
from agg a
cross join anchor an
