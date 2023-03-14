select
    dbt_scd_id as sk_employee,
    employee_id,
    employee_name,
    region,
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to,
    dbt_updated_at as last_updated_at
from {{ ref("sp_employee") }}
