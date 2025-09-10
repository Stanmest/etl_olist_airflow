with enriched as (
  select * from {{ ref('fact_order_items_enriched') }}
)
select
  date_trunc('day', order_purchase_timestamp)::date as order_date,
  sum(price) as revenue,
  sum(freight_value) as freight_cost,
  count(distinct order_id) as orders
from enriched
group by 1
order by 1
