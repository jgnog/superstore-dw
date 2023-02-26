with union_date as (
    select distinct (order_date)
from {{ref('stg_order')}}
union
    select distinct (ship_date)
from {{ref('stg_shipment')}}
order by order_date
)



select 
    order_date as date_DATE,
    extract (day from order_date) as day,
    extract (month from order_date) as month,
    extract (year from order_date) as year,
    to_char (order_date, 'Month') as month_name,
    to_char (order_date, 'Mon') as month_name_short,
    date_part('week', order_date) as week_number,
    to_char( order_date,'Day') as day_of_week ,--sendo 0 (zero) o domingo, 1 (um) a segunda-feira
    CEILING(EXTRACT(MONTH FROM order_date) / 3.0) AS quarter,-- CEILING() arredonda para cima
    CASE
        WHEN date_part('dow', order_date)= 0 THEN true --sunday
        when date_part('dow', order_date)= 6 THEN TRUE--Saturday
        ELSE FALSE
    END AS is_weekend

from union_date
