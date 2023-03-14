select
    order_id,
    order_date,
    customer_id
from {{ source("norm", "t_order") }}
