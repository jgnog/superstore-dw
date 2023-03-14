select
    ol.order_line_id,
    ol.order_id,
    o.order_date,
    o.customer_id,
    ol.sales,
    ol.quantity,
    ol.discount,
    ol.profit
from {{ source("norm", "t_order_line") }} ol
    join {{ source("norm", "t_order") }} o on ol.order_id = o.order_id
