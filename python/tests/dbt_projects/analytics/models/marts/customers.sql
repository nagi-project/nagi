{{
    config(
        tags=['daily']
    )
}}

with orders as (
    select
        customer_id,
        count(*) as order_count,
        sum(amount) as total_amount
    from {{ ref('stg_orders') }}
    group by customer_id
)

select
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    coalesce(o.order_count, 0) as order_count,
    coalesce(o.total_amount, 0) as total_amount
from {{ ref('stg_customers') }} c
left join orders o on c.customer_id = o.customer_id
