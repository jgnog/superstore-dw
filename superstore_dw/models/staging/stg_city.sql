select
    c.city_id,
    c.city as city_name,
    s.state,
    r.region,
    ct.country
from {{ source("norm", "t_city") }} c
    join {{ source("norm", "t_state") }} s on c.state_id = s.state_id
    join {{ source("norm", "t_country") }} ct on c.country_id = ct.country_id
    join {{ source("norm", "t_region") }} r on c.region_id = r.region_id
