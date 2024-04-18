select
    c.customer_id,
    c.name as customer_name,
    s.segment
from {{ source("norm", "t_customer") }} c
    join {{ source("norm", "t_segment") }} s on s.segment_id = c.segment_id
