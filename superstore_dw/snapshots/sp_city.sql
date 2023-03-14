{% snapshot sp_city %}

{{
    config(
        unique_key='city_id',
        strategy='check',
        check_cols=['region']
    )
}}

select
    *
from {{ ref('stg_city') }}

{% endsnapshot %}
