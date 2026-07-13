-- Singular test: RFM phải hợp lệ (recency>=0, frequency>=1, monetary>=0).
select *
from {{ ref('fact_rfm_segmentation') }}
where recency < 0 or frequency < 1 or monetary < 0
