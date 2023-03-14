select
    ol.order_line_id,
    ol.order_id,
    ol.sales,
    ol.quantity,
    ol.discount,
    ol.profit
from {{ source("norm", "t_order_line") }} ol
