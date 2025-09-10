{{ config(
    materialized='incremental',
    unique_key=['order_id','product_id']
) }}

with oi as (
  select * from {{ ref('stg_order_items') }}
),
o as (
  select * from {{ ref('stg_orders') }}
),
p as (
  select * from {{ ref('stg_products') }}
)

select
  oi.order_id,
  oi.product_id,
  oi.seller_id,
  o.order_purchase_timestamp,
  oi.shipping_limit_date,
  oi.price,
  oi.freight_value,
  p.product_category_name
from oi
join o using (order_id)
left join p using (product_id)

{% if is_incremental() %}
  where o.order_purchase_timestamp >
    (select coalesce(max(order_purchase_timestamp), '1900-01-01'::timestamp)
     from {{ this }})
{% endif %}
