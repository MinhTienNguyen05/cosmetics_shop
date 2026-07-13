-- Singular test: price phải > 0 ở Silver (return violating rows -> fail nếu có).
select *
from {{ ref('silver_events') }}
where price <= 0
