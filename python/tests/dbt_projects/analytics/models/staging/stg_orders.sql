select
    order_id,
    customer_id,
    amount,
    status,
    ordered_at
from {{ source('ecommerce', 'orders') }}
