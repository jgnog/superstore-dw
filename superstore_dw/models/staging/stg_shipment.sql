select
    s.shipment_id,
    s.order_id,
    sm.ship_mode,
    s.city_id,
    ship_date
from {{ source("norm", "t_shipment") }} s
    join {{ source("norm", "t_ship_mode") }} sm on s.ship_mode_id = sm.ship_mode_id

