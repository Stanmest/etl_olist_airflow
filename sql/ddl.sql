CREATE SCHEMA IF NOT EXISTS olist;

CREATE TABLE IF NOT EXISTS olist.dim_customers (
  customer_key SERIAL PRIMARY KEY,
  customer_id TEXT UNIQUE,
  customer_unique_id TEXT,
  customer_city TEXT,
  customer_state TEXT
);

CREATE TABLE IF NOT EXISTS olist.dim_orders (
  order_key SERIAL PRIMARY KEY,
  order_id TEXT UNIQUE,
  order_status TEXT,
  order_purchase_timestamp TIMESTAMP,
  order_approved_at TIMESTAMP,
  order_delivered_carrier_date TIMESTAMP,
  order_delivered_customer_date TIMESTAMP,
  order_estimated_delivery_date TIMESTAMP
);

CREATE TABLE IF NOT EXISTS olist.dim_products (
  product_key SERIAL PRIMARY KEY,
  product_id TEXT UNIQUE,
  product_category_name TEXT,
  product_weight_g NUMERIC,
  product_length_cm NUMERIC,
  product_height_cm NUMERIC,
  product_width_cm NUMERIC
);

CREATE TABLE IF NOT EXISTS olist.fact_order_items (
  order_item_key SERIAL PRIMARY KEY,
  order_id TEXT REFERENCES olist.dim_orders(order_id),
  product_id TEXT REFERENCES olist.dim_products(product_id),
  seller_id TEXT,
  shipping_limit_date TIMESTAMP,
  price NUMERIC,
  freight_value NUMERIC
);

CREATE TABLE IF NOT EXISTS olist.fact_payments (
  payment_key SERIAL PRIMARY KEY,
  order_id TEXT REFERENCES olist.dim_orders(order_id),
  payment_sequential INT,
  payment_type TEXT,
  payment_installments INT,
  payment_value NUMERIC
);
