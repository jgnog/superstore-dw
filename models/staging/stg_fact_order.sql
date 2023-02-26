with order_line as (
    select 
    distinct ol.order_id,
    o.order_date,
    count(*) over(partition by ol.order_id) as nr_of_order_lines,
    sum(quantity) over(partition by ol.order_id) as quantity,
    sum(sales) over(partition by ol.order_id) as sales,
    sum(profit) over (partition by ol.order_id) as profit
from {{ref('stg_order_line')}} ol
    right join {{ref ('stg_order')}}  o on ol.order_id=o.order_id
),
shipment as (
    select
    ol.*,
    s.ship_date,
    DATE_PART('day', AGE(s.ship_date, ol.order_date))as time_to_ship_days,--(s.ship_date-o.order_date)
    s.ship_mode
    from order_line ol
    left join {{ref('stg_shipment')}} s on ol.order_id=s.order_id
)
select 
	s.*,
	case 
        when s.ship_mode is null and s.time_to_ship_days is null then 'pending'
        else 'shipped'
    end as order_status
from shipment s