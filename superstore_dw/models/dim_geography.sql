select
    dbt_scd_id as sk_geography,
    city_id,
    city_name,
    state,
    country,
    region,
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to,
    dbt_updated_at as last_updated_at
from {{ ref("sp_city") }}
