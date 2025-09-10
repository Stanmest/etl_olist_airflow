import io, pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text as _text  # not required but safe to import
DDL_PATH = "/opt/airflow/sql/ddl.sql"
DW_CONN = "postgresql+psycopg2://dwh:dwh@dw-postgres:5432/olist_dw"
def _df(json_str):
    return pd.read_json(io.StringIO(json_str), orient="records")
def load_to_dw(dim_customers_json, dim_orders_json, dim_products_json, fact_order_items_json, fact_payments_json):
    engine = create_engine(DW_CONN)
    with engine.begin() as conn:
        with open(DDL_PATH, "r") as f:
            conn.exec_driver_sql(f.read())
    with engine.begin() as conn:
        _df(dim_customers_json).to_sql("dim_customers", conn, schema="olist", if_exists="append", index=False)
        _df(dim_orders_json).to_sql("dim_orders", conn, schema="olist", if_exists="append", index=False)
        _df(dim_products_json).to_sql("dim_products", conn, schema="olist", if_exists="append", index=False)
        _df(fact_order_items_json).to_sql("fact_order_items", conn, schema="olist", if_exists="append", index=False)
        _df(fact_payments_json).to_sql("fact_payments", conn, schema="olist", if_exists="append", index=False)
    return "Loaded to olist_dw"
