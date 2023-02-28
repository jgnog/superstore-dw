{% snapshot sp_customer= check %}

{{
    config(
        target_schema='snapshots',
        unique_key='customer_id',
        strategy='check',
        check_cols=['customer_name', 'segment']
    )
}}

select
    c.*,
    CURRENT_TIMESTAMP as snapshot_date
from {{ ref('stg_customer') }}c

{% endsnapshot %}
