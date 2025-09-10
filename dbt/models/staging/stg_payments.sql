with src as (
  select * from {{ source('olist', 'fact_payments') }}
)
select
  order_id,
  payment_sequential,
  payment_type,
  payment_installments,
  payment_value::numeric
from src
