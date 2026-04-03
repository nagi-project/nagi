select
    customer_id,
    first_name,
    last_name,
    email
from {{ ref('customers_raw') }}
