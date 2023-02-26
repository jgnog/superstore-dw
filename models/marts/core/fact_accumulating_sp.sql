with data_order as (
    select
        d.sk_date,
        o.order_date
    from {{ref ('dim_date')}} d
        join {{ref('stg_order')}} o on o.order_date=d.date_date
),
data_ship as (
    select
        d.sk_date,
        s.ship_date
    from {{ref ('dim_date')}} d
        join {{ref('stg_shipment')}} s on s.ship_date=d.date_date
), 
customer as (
    select
        c.sk_customer,
        o.order_id
        from {{ref('stg_order')}} o
            join {{ref('dim_customer')}} c on c.customer_id=o.customer_id
), 
geograph as (
    select
        g.sk_geography,
        s.order_id
        from {{ref('stg_shipment')}} s
            join {{ref('dim_geography')}} g on g.city=s.city
),
employee as (
    select
        e.sk_employee,
        s.region_id,
        s.order_id
        from {{ref('stg_shipment')}} s
            join {{ref('stg_employee')}} se on se.region_id=s.region_id
            join {{ref('dim_employee')}} e on e.employee_id=se.employee_id
), 
final as (
    select 
        spo.order_id,
        o.sk_date as sk_order_date,
        s.sk_date as sk_shipped_date,
        c.sk_customer,
        g.sk_geography,
        e.sk_employee,
        spo.order_status,
        spo.ship_mode,
        spo.nr_of_order_lines,
        spo.quantity,
        spo.time_to_ship_days,
        spo.sales,
        spo.profit,
        spo.snapshot_date as created_at,
        ROW_NUMBER() OVER ( partition by spo.order_id  ORDER BY spo.snapshot_date asc) AS row_nr
        
    from {{ref ('sp_fact_order')}} spo
         join data_order o on o.order_date=spo.order_date
         join data_ship s on s.ship_date=spo.ship_date
         join customer c on c.order_id=spo.order_id
         join geograph g on g.order_id=spo.order_id
        left join employee e on e.order_id=spo.order_id
)
select 
    f.order_id,
    f.sk_order_date,
    f.sk_customer,
    f.sk_geography,
    f.sk_employee,
    f.order_status,
    f.ship_mode,
    f.nr_of_order_lines,
    f.quantity,
    f.time_to_ship_days,
    f.sales,
    f.profit,
    f.created_at
    
from final f
where row_nr=1

