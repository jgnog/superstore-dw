WITH transactions AS (
    select 
    *
    from {{ref ('sp_stg_fact_transactional')}}
),
customer as (
    select *,
        case
            when valid_to is null then (CURRENT_TIMESTAMP) --atual_date
            else valid_to
        end as valid_to_2
    from {{ref ('dim_customer')}}
    
), 
dates as (
    select *
    from {{ref ('dim_date')}} 

),
geography as (
    select *,
        case
            when valid_to is null then (CURRENT_TIMESTAMP) --atual_date
            else valid_to
        end as valid_to_2
    from {{ref ('dim_geography')}} 
),
employee as (
    select *,
        case
            when valid_to is null then (CURRENT_TIMESTAMP) --atual_date
            else valid_to
        end as valid_to_2
    from {{ref ('dim_employee')}} 
)

SELECT 
    t.dbt_scd_id as sk_transaction,
    d.sk_date as sk_order_date,
    d1.sk_date as sk_shipped_date,
    g.sk_geography,
    d.sk_date,
    sk_employee,
    t.order_line_id,
    t.order_id,
    t.ship_mode_id,
    t.customer_id,
    t.city_id,
    t.region_id,
    t.country_id,
    t.postal_code_id,
    t.product_variation_id,
    t.sales,
    t.quantity,
    t.discount,
    t.profit,
    t.snapshot_date,
    t.dbt_updated_at as updated_at,
    t.dbt_valid_from as valid_from,
    t.dbt_valid_to as valid_to

    FROM transactions t
        JOIN customer c ON 
            t.customer_id = c.customer_id 
           AND t.dbt_valid_from  BETWEEN c.valid_from AND c.valid_to_2
        JOIN dates d ON
            t.order_date = d.date_date
        LEFT JOIN dates d1 ON
            t.ship_date = d1.date_date
        LEFT JOIN geography g ON
        t.city_id = g.city_id
            AND t.dbt_valid_from  BETWEEN g.valid_from AND g.valid_to_2
        LEFT JOIN employee e ON
        t.employee_id = e.employee_id
            AND t.dbt_valid_from  BETWEEN e.valid_from AND e.valid_to_2

