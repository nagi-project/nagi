select
    customer_id,
    first_name,
    last_name,
    email
from {{ ref('stg_customers') }}
