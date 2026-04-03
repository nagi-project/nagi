{{
    config(
        tags=['daily']
    )
}}

select
    category,
    count(*) as product_count,
    avg(price) as avg_price
from {{ ref('stg_products') }}
group by category
