select 
    ROW_NUMBER() OVER (order by date_date) AS sk_date,
    d.*
    from {{ref('stg_date')}} as d