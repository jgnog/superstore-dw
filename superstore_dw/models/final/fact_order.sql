with dimension_ids as (
    select
        o.order_id,
        o.order_date,
        ship.ship_date,
        o.customer_id,
        ship.city_id,
        case
            when ret.return_id is not null then 'Returned'
            when ship.shipment_id is not null then 'Shipped'
            else 'Ordered'
        end as order_status,
        date_part('day', ship.ship_date::timestamp - o.order_date::timestamp) as time_to_ship_days,
        ship.ship_mode,
        aol.nr_of_order_lines,
        aol.quantity,
        aol.sales_wo_discount,
        aol.sales,
        aol.discount,
        aol.profit
    from {{ ref('stg_order') }} o
        join {{ ref('int_agg_order_line') }} aol on o.order_id = aol.order_id
        left join {{ ref('stg_shipment') }} ship on o.order_id = ship.order_id
        left join {{ ref('stg_return') }} ret on o.order_id = ret.order_id
),

surrogate_keys as (
    select
        order_id,
        order_dd.sk_date as sk_order_date,
        ship_dd.sk_date as sk_ship_date,
        dcust.sk_customer as sk_customer,
        dgeo.sk_geography as sk_geography,
        demp.sk_employee as sk_employee,
        order_status,
        time_to_ship_days,
        ship_mode,
        nr_of_order_lines,
        quantity,
        sales_wo_discount,
        sales,
        discount,
        profit
    from dimension_ids dids
        join {{ ref('dim_date') }} order_dd on dids.order_date = order_dd.date
        left join {{ ref('dim_date') }} ship_dd on dids.ship_date = ship_dd.date
        join {{ ref('dim_customer') }} dcust on dids.customer_id = dcust.customer_id
            and dids.order_date between dcust.valid_from and dcust.valid_to
        left join {{ ref('dim_geography') }} dgeo on dids.city_id = dgeo.city_id
            and dids.order_date between dgeo.valid_from and dgeo.valid_to
        left join {{ ref('dim_employee') }} demp on dgeo.region = demp.region
            and dids.order_date between demp.valid_from and demp.valid_to
),

final as (
    select
        order_id,
        sk_order_date,
        sk_ship_date,
        sk_customer,
        sk_geography,
        sk_employee,
        order_status,
        time_to_ship_days,
        ship_mode,
        nr_of_order_lines,
        quantity,
        sales_wo_discount,
        sales,
        discount,
        profit
    from surrogate_keys
)

select
    *
from final
