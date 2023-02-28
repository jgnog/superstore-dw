with fact as (select * from {{ ref("sp_dim_order") }})

select
    order_id,
    sk_order_date,
    sk_shipped_date,
    sk_customer,
    sk_geography,
    sk_employee,
    order_status,
    ship_mode,
    nr_of_order_lines,
    quantity,
    time_to_ship_days,
    sales,
    profit,
    snapshot_date as created_at,
    dbt_valid_from as last_updated_at

from fact
where dbt_valid_to is null
