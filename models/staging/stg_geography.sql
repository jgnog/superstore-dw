select 
    c.city_id,
    c.city,
    s.state,
    r.region,
    co.country
from {{source ( 'norm', 't_city')}} c 
    right join {{source ( 'norm', 't_state')}} s on s.state_id=c.state_id
    right join {{source ( 'norm', 't_region')}} r on r.region_id=c.region_id
    right join {{source ( 'norm', 't_country')}} co on co.country_id=c.country_id

