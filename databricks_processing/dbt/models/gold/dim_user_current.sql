{{
    config(
        materialized='table',
        file_format='delta'
    )
}}

-- Trạng thái HIỆN TẠI của user (segment RFM). Nguồn cho snapshot SCD2.
-- Bao gồm mọi user (purchaser + non-purchaser=Prospect).
with silver as (
    select * from {{ ref('silver_events') }}
),
all_users as (
    select distinct user_id from silver
),
purchases as (
    select * from silver where event_type = 'purchase'
),
anchor as (
    select max(to_date(event_time)) as rfm_anchor from purchases
),
purchase_agg as (
    select
        user_id,
        min(to_date(event_time)) as first_purchase_date,
        max(to_date(event_time)) as last_purchase_date,
        count(distinct user_session) as frequency,
        sum(price) as monetary
    from purchases
    group by user_id
)
select
    a.user_id,
    p.first_purchase_date,
    coalesce(p.frequency, 0) as frequency,
    coalesce(p.monetary, 0) as monetary,
    datediff(an.rfm_anchor, p.last_purchase_date) as recency,
    case
        when p.last_purchase_date is null then 'Prospect'
        when datediff(an.rfm_anchor, p.last_purchase_date) <= 30
             and p.frequency >= 3 and p.monetary > 50 then 'VIP'
        when datediff(an.rfm_anchor, p.last_purchase_date) <= 30
             and p.frequency = 1 then 'Newbie'
        when datediff(an.rfm_anchor, p.last_purchase_date) > 60 then 'Churning'
        else 'Regular'
    end as lifetime_segment
from all_users a
left join purchase_agg p on a.user_id = p.user_id
cross join anchor an
