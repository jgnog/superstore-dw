{% snapshot sp_employee= 'timestamp' %}

{{
    config(
        target_schema='snapshots',
        unique_key='employee_id',
        strategy='check',
        check_cols=['name', 'region']
    )
}}

select
    e.*,
    CURRENT_TIMESTAMP as snapshot_date
from {{ ref('stg_employee') }}e

{% endsnapshot %}