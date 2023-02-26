{% snapshot sp_geography= 'timestamp' %}

{{
    config(
        target_schema='snapshots',
        unique_key='city_id',
        strategy='check',
        check_cols=['city', 'state','region','country']
    )
}}

select
    g.*,
    CURRENT_TIMESTAMP  as snapshot_date
from {{ ref('stg_geography') }} g

{% endsnapshot %}