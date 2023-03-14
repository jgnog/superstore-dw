{% snapshot sp_employee %}

{{
    config(
        unique_key='employee_id',
        strategy='check',
        check_cols=['employee_name', 'region']
    )
}}

select
    *
from {{ ref('stg_employee') }}

{% endsnapshot %}
