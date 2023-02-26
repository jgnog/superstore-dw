select 
	s.shipment_id,
    c.region_id,
	s.order_id,
	s.ship_date,
	s.city_id,
	c.city,
	s.postal_code_id,
	p.postal_code,
	s.ship_mode_id,
	sm.ship_mode
from norm.t_shipment s
    left join {{source('norm','t_postal_code')}} p on p.postal_code_id=s.postal_code_id
    left join {{source('norm','t_ship_mode')}} sm on sm.ship_mode_id=s.ship_mode_id
    left join {{source ('norm','t_city')}}  c on c.city_id=s.city_id