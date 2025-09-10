with src as (
  select * from {{ source('olist', 'dim_orders') }}
)
select
  order_id,
  order_status,
  order_purchase_timestamp,
  order_approved_at,
  order_delivered_carrier_date,
  order_delivered_customer_date,
  order_estimated_delivery_date
from src
