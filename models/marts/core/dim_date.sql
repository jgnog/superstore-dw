select 
    ROW_NUMBER() OVER (ORDER BY date_date) AS sk_date,
    d.*
from {{ref('stg_date')}} d