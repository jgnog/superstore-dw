{% snapshot sp_stg_fact_order %}

    {{
        config(
            target_schema='snapshots',
            strategy='check',
            unique_key='order_id',
            check_cols=['ship_date','order_id']
        )
    }}

select
    o.*,
    CURRENT_TIMESTAMP as snapshot_date
from {{ ref('stg_fact_order') }} o

{% endsnapshot %}