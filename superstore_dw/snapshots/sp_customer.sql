{% snapshot sp_customer %}

{{
    config(
        unique_key='customer_id',
        strategy='check',
        check_cols=['customer_name', 'segment']
    )
}}

select
    *
from {{ ref('stg_customer') }}

{% endsnapshot %}
