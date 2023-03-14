select
    order_line_id,
    order_id,
    sales,
    quantity,
    discount,
    profit,
    sales / (1 - discount) as sales_wo_discount
from {{ ref('stg_order_line') }}
