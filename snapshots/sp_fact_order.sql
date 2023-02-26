{% snapshot sp_fact_order= 'timestamp' %}

{{
    config(
        target_schema='snapshots',
        strategy='timestamp',
        unique_key='order_id',
        updated_at='ship_date',
    )
}}
select
    fo.*,
    CURRENT_TIMESTAMP as snapshot_date
from {{ ref('stg_fact_order') }} fo

{% endsnapshot %}