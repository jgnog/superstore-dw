select
    dbt_scd_id as sk_employee,
    employee_id,
    name as employee_name,
    dbt_valid_from  as valid_from,
    dbt_valid_to  as valid_to,
    snapshot_date as created_at,
    dbt_updated_at as last_updated_at
from {{ ref("sp_employee") }}
