select 
    *
from norm.t_shipment 
    left join {{source('norm','t_postal_code')}} using (postal_code_id) 
    left join {{source('norm','t_ship_mode')}} using (ship_mode_id)
    left join {{source ('norm','t_city')}}  using (city_id) 
