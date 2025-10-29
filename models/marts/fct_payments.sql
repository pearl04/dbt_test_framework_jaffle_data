-- build facts table at payments level

select
  payment_id,                 -- grain
  order_id,
  customer_id,
  payment_method,
  payment_amount,
  order_status,              
  order_date
from {{ ref('int_payments_clean') }}
