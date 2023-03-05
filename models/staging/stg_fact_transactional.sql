select 
*
from {{ref ('stg_order_line')}}
    RIGHT JOIN {{ref ('stg_order')}} using (order_id)
    LEFT JOIN {{ref ('stg_shipment')}} using (order_id)
    LEFT JOIN {{ref ('stg_employee')}} using (region_id)