select
    dbt_scd_id as sk_geography,
    city,
    state,
    region,
    country,
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to,
    snapshot_date as created_at,
    dbt_updated_at as last_updated_at
from {{ ref("sp_geography") }}
