with
    series_dates as (
        select
            generate_series(
               ( select min(order_date) as first_date from {{ source ( 'norm', 't_order')}}),
                (select max(order_date) as last_date from {{ source ( 'norm', 't_order')}}),
                '1 day'::interval
            )::date as order_date
)
select
    order_date as date_date,
    extract(day from order_date) as day,
    extract(month from order_date) as month,
    extract(year from order_date) as year,
    to_char(order_date, 'Month') as month_name,
    to_char(order_date, 'Mon') as month_name_short,
    date_part('week', order_date) as week_number,
    -- sendo 0 (zero) o domingo, 1 (um) a segunda-feira
    to_char(order_date, 'Day') as day_of_week,
    -- CEILING() arredonda para cima
    ceiling(extract(month from order_date) / 3.0) as quarter,
    case
        when date_part('dow', order_date) = 0  -- sunday
        then true
        when date_part('dow', order_date) = 6  -- Saturday
        then true
        else false
    end as is_weekend

from series_dates
