with src as (
  select * from {{ source('olist', 'fact_order_items') }}
)
select
  order_id,
  product_id,
  seller_id,
  shipping_limit_date,
  price::numeric,
  freight_value::numeric
from src
