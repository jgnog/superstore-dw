select
    order_id,
    count(order_line_id) as nr_of_order_lines,
    sum(quantity) as quantity,
    sum(sales_wo_discount)::numeric(8,3) as sales_wo_discount,
    sum(sales)::numeric(8,3) as sales,
    (1 - (sum(sales) / sum(sales_wo_discount)))::numeric(3,3) as discount,
    sum(profit)::numeric(8,3) as profit
from {{ ref('int_order_line') }}
group by order_id
