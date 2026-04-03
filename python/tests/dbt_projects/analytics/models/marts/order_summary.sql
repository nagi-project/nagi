{{
    config(
        materialized='table',
        tags=['daily', 'finance']
    )
}}

select
    status,
    count(*) as order_count,
    sum(amount) as total_amount,
    avg(amount) as avg_amount
from {{ ref('stg_orders') }}
group by status
