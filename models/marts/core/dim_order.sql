with
q1 as (
    select 
        o.order_id,
        d1.sk_date as sk_order_date,
        d.sk_date as sk_shipped_date,
        c.sk_customer,
        g.sk_geography,
        e.sk_employee,
        case 
        when o.ship_mode is null and DATE_PART('day', AGE(o.ship_date, o.order_date)) is null 
            then 'pending'
            else 'shipped'
        end as order_status,
        o.ship_mode,
        o.nr_of_order_lines,
        o.quantity,
        DATE_PART('day', AGE(o.ship_date, o.order_date))as time_to_ship_days,--(s.ship_date-o.order_date)
        o.sales,
        o.profit
    from {{ref ('stg_fact_order') }} o
    left join {{ref('dim_date')}} d1 on o.order_date=d1.date_date
    left join {{ref('dim_date')}} d on o.ship_date=d.date_date
    join {{ref ('dim_customer')}} c using (customer_id) 
    left join {{ref ('dim_geography')}} g using (city_id)
    left join {{ref ('dim_employee')}} e on e.employee_id= o.employee_id
)
select *
from q1