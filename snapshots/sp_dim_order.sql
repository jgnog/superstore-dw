{% snapshot sp_dim_order = 'check' %}

    {{
        config(
            target_schema='snapshots',
            strategy='check',
            unique_key='order_id',
            check_cols=['order_status', 'ship_mode', 'sk_order_date','sk_shipped_date'],
        )
    }}

select
    o.*,
    CURRENT_TIMESTAMP as snapshot_date
from {{ ref('dim_order') }} o

{% endsnapshot %}