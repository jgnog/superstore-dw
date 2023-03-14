with first_return as (
    select
        r.return_id,
        r.order_id,
        row_number() over(partition by r.order_id order by r.return_id) as row_nr
    from {{ source("norm", "t_return") }} r
)
select
    return_id,
    order_id
from first_return
where row_nr = 1
