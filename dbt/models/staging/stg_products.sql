with src as (
  select * from {{ source('olist', 'dim_products') }}
)
select
  product_id,
  product_category_name,
  product_weight_g::numeric,
  product_length_cm::numeric,
  product_height_cm::numeric,
  product_width_cm::numeric
from src
