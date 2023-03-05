with fact as (
    select *,
        row_number() over ( partition by( order_id ) order by snapshot_date Desc ) as nr_row
    from {{ ref('sp_stg_fact_order') }} 
) ,
customer as (
    select *,
        case
            when valid_to is null then (CURRENT_TIMESTAMP) --atual_date
            else valid_to
        end as valid_to_2
    from {{ref ('dim_customer')}} 
),
employee as (
    select *,
        case
            when valid_to is null then (CURRENT_TIMESTAMP) --atual_date
            else valid_to
        end as valid_to_2
    from {{ref ('dim_employee')}} 
    
),
geography as (
    select *,
        case
            when valid_to is null then (CURRENT_TIMESTAMP) --atual_date
            else valid_to
        end as valid_to_2
    from {{ref ('dim_geography')}} 
), 
dates as (
    select *
    from {{ref ('dim_date')}} 

)
select
    f.order_id,
    d.sk_date as sk_order_date,
    d1.sk_date as sk_shipped_date,
    e.sk_employee,
    c.sk_customer,
    g.sk_geography,
    f.order_status,
    f.ship_mode,
    f.nr_of_order_lines,
    f.quantity,
    f.time_to_ship_days,
    f.sales,
    f.profit,
    f.snapshot_date as created_at,
    f.dbt_updated_at as last_updated_at

from fact f
    JOIN customer c ON 
        f.customer_id = c.customer_id 
           AND f.dbt_valid_from  BETWEEN c.valid_from AND c.valid_to_2
    JOIN employee e ON
        f.employee_id = e.employee_id
            AND f.dbt_valid_from  BETWEEN e.valid_from AND e.valid_to_2
    JOIN geography g ON
        f.city_id = g.city_id
            AND f.dbt_valid_from  BETWEEN g.valid_from AND g.valid_to_2
    JOIN dates d ON
        f.order_date = d.date_date
    JOIN dates d1 ON
        f.ship_date = d1.date_date    
nr_row= 1  
