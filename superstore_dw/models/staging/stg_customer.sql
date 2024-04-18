select
    e.customer_id,
    e.name as customer_name,
    s.segment
from {{ source("norm", "t_customer") }} e
    join {{ source("norm", "t_segment") }} s on s.segment_id = s.segment_id
