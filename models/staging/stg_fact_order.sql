with
order_line as (
    select 
    order_id,
    count(*) as nr_of_order_lines,
    sum(quantity) as quantity,
    sum(sales) as sales,
    sum(profit) as profit
    from {{ref('stg_order_line')}}
    group by order_id
),
orders as (
    select * 
    from {{ref('stg_order')}}
),
shipment as (
    select 
        s.ship_mode_id,
        s.postal_code_id,
        s.shipment_id,
        s.order_id,
        s.ship_date,
        s.postal_code,
        s.ship_mode,
        g.*,
        e.employee_id
    from {{ref('stg_shipment')}} as  s
    left join {{ref('stg_geography')}} as g on s.city_id = g.city_id
    left join {{ref ('stg_employee')}} as e on e.region_id = s.region_id
),
final as (
    select *
    from orders
    left join shipment using (order_id)
    join order_line using (order_id)
)  
select 
    *
from final
