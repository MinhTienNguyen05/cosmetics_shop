{% snapshot dim_user_snapshot %}

{{
    config(
        target_database='workspace',
        target_schema='gold_cosmetics',
        file_format='delta',
        unique_key='user_id',
        strategy='check',
        check_cols=['lifetime_segment']
    )
}}

-- SCD Type 2 trên lifetime_segment: khi segment đổi -> dbt đóng row cũ, mở row mới.
-- Bảng đích = workspace.gold_cosmetics.dim_user_snapshot (chính là dim_user SCD2).
select * from {{ ref('dim_user_current') }}

{% endsnapshot %}
