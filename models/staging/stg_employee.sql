select 
    e.*,
    r.region
from {{source('norm', 't_employee')}} e
    join {{source('norm','t_region')}} r on r.region_id=e.region_id