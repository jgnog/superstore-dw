select 
   *
from {{source ( 'norm', 't_city')}}  
    right join {{source ( 'norm', 't_state')}} using (state_id)
    right join {{source ( 'norm', 't_region')}} using (region_id)
    right join {{source ( 'norm', 't_country')}} using (country_id)