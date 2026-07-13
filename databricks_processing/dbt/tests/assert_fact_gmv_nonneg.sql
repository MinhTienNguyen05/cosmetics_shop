-- Singular test: GMV và orders phải >= 0.
select *
from {{ ref('fact_daily_performance') }}
where total_gmv < 0 or total_orders < 0
