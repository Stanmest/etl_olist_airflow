import io, os
import pandas as pd
from sqlalchemy import create_engine, text

DDL_PATH = "/opt/airflow/sql/ddl.sql"
DW_URI = "postgresql+psycopg2://dwh:dwh@dw-postgres:5432/olist_dw"

def _df(obj):
    # Accept parquet paths or JSON strings (backwards compatible)
    if isinstance(obj, str) and obj.endswith(".parquet") and os.path.exists(obj):
        return pd.read_parquet(obj)
    return pd.read_json(io.StringIO(obj), orient="records")

def _copy_df(engine, table, cols, df, schema="olist"):
    """Fast load using COPY FROM STDIN (psycopg2)."""
    raw = engine.raw_connection()
    try:
        cur = raw.cursor()
        buf = io.StringIO()
        # write rows in the exact column order, no header
        df = df[cols]
        df.to_csv(buf, index=False, header=False)
        buf.seek(0)
        cur.copy_expert(
            f"COPY {schema}.{table} ({','.join(cols)}) FROM STDIN WITH (FORMAT csv)",
            buf
        )
        raw.commit()
    finally:
        raw.close()

def load_to_dw(dim_customers_src, dim_orders_src, dim_products_src, fact_order_items_src, fact_payments_src):
    dc = _df(dim_customers_src)
    do = _df(dim_orders_src)
    dp = _df(dim_products_src)
    fi = _df(fact_order_items_src)
    fp = _df(fact_payments_src)

    print(f"[load] rows dc={len(dc)} do={len(do)} dp={len(dp)} fi={len(fi)} fp={len(fp)}")

    engine = create_engine(DW_URI)

    # 0) Ensure schema/tables exist
    with engine.begin() as conn:
        with open(DDL_PATH, "r") as f:
            conn.exec_driver_sql(f.read())

    # 1) Create staging tables (once) and truncate each time
    with engine.begin() as conn:
        conn.exec_driver_sql("""
        CREATE TABLE IF NOT EXISTS olist._tmp_dim_customers(
          customer_id TEXT PRIMARY KEY,
          customer_unique_id TEXT,
          customer_city TEXT,
          customer_state TEXT
        );
        TRUNCATE olist._tmp_dim_customers;
        """)
        conn.exec_driver_sql("""
        CREATE TABLE IF NOT EXISTS olist._tmp_dim_orders(
          order_id TEXT PRIMARY KEY,
          order_status TEXT,
          order_purchase_timestamp TIMESTAMP,
          order_approved_at TIMESTAMP,
          order_delivered_carrier_date TIMESTAMP,
          order_delivered_customer_date TIMESTAMP,
          order_estimated_delivery_date TIMESTAMP
        );
        TRUNCATE olist._tmp_dim_orders;
        """)
        conn.exec_driver_sql("""
        CREATE TABLE IF NOT EXISTS olist._tmp_dim_products(
          product_id TEXT PRIMARY KEY,
          product_category_name TEXT,
          product_weight_g NUMERIC,
          product_length_cm NUMERIC,
          product_height_cm NUMERIC,
          product_width_cm NUMERIC
        );
        TRUNCATE olist._tmp_dim_products;
        """)

    # 2) COPY into staging tables
    _copy_df(engine, "_tmp_dim_customers",
             ["customer_id","customer_unique_id","customer_city","customer_state"], dc)
    _copy_df(engine, "_tmp_dim_orders",
             ["order_id","order_status","order_purchase_timestamp","order_approved_at",
              "order_delivered_carrier_date","order_delivered_customer_date","order_estimated_delivery_date"], do)
    _copy_df(engine, "_tmp_dim_products",
             ["product_id","product_category_name","product_weight_g","product_length_cm","product_height_cm","product_width_cm"], dp)

    # 3) UPSERT from staging into dimension tables
    with engine.begin() as conn:
        conn.exec_driver_sql("""
        INSERT INTO olist.dim_customers (customer_id, customer_unique_id, customer_city, customer_state)
        SELECT customer_id, customer_unique_id, customer_city, customer_state
        FROM olist._tmp_dim_customers
        ON CONFLICT (customer_id) DO UPDATE SET
          customer_unique_id = EXCLUDED.customer_unique_id,
          customer_city      = EXCLUDED.customer_city,
          customer_state     = EXCLUDED.customer_state;
        """)
        conn.exec_driver_sql("""
        INSERT INTO olist.dim_orders (
          order_id, order_status, order_purchase_timestamp, order_approved_at,
          order_delivered_carrier_date, order_delivered_customer_date, order_estimated_delivery_date
        )
        SELECT order_id, order_status, order_purchase_timestamp, order_approved_at,
               order_delivered_carrier_date, order_delivered_customer_date, order_estimated_delivery_date
        FROM olist._tmp_dim_orders
        ON CONFLICT (order_id) DO UPDATE SET
          order_status                 = EXCLUDED.order_status,
          order_purchase_timestamp     = EXCLUDED.order_purchase_timestamp,
          order_approved_at            = EXCLUDED.order_approved_at,
          order_delivered_carrier_date = EXCLUDED.order_delivered_carrier_date,
          order_delivered_customer_date= EXCLUDED.order_delivered_customer_date,
          order_estimated_delivery_date= EXCLUDED.order_estimated_delivery_date;
        """)
        conn.exec_driver_sql("""
        INSERT INTO olist.dim_products (
          product_id, product_category_name, product_weight_g,
          product_length_cm, product_height_cm, product_width_cm
        )
        SELECT product_id, product_category_name, product_weight_g,
               product_length_cm, product_height_cm, product_width_cm
        FROM olist._tmp_dim_products
        ON CONFLICT (product_id) DO UPDATE SET
          product_category_name = EXCLUDED.product_category_name,
          product_weight_g      = EXCLUDED.product_weight_g,
          product_length_cm     = EXCLUDED.product_length_cm,
          product_height_cm     = EXCLUDED.product_height_cm,
          product_width_cm      = EXCLUDED.product_width_cm;
        """)

    # 4) Facts: truncate + COPY (idempotent, fastest)
    with engine.begin() as conn:
        conn.exec_driver_sql("TRUNCATE olist.fact_order_items, olist.fact_payments RESTART IDENTITY;")

    _copy_df(engine, "fact_order_items",
             ["order_id","product_id","seller_id","shipping_limit_date","price","freight_value"], fi)
    _copy_df(engine, "fact_payments",
             ["order_id","payment_sequential","payment_type","payment_installments","payment_value"], fp)

    print("[load] done")
    return "Upserted dims and reloaded facts via COPY"
