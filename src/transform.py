import io
from pathlib import Path
import pandas as pd

PROCESSED = Path("/opt/airflow/data/processed")
PROCESSED.mkdir(parents=True, exist_ok=True)

def _from_xcom(json_str):
    # backward compat: allow both JSON payloads and file paths
    if isinstance(json_str, str) and json_str.endswith(".parquet"):
        return pd.read_parquet(json_str)
    return pd.read_json(io.StringIO(json_str), orient="records")

def transform_olist(customers_json, orders_json, order_items_json, products_json, payments_json):
    customers = _from_xcom(customers_json)
    orders = _from_xcom(orders_json)
    order_items = _from_xcom(order_items_json)
    products = _from_xcom(products_json)
    payments = _from_xcom(payments_json)

    # normalize columns
    for df in [customers, orders, order_items, products, payments]:
        df.columns = [c.strip().lower() for c in df.columns]

    # timestamps
    ts_cols_orders = [
        "order_purchase_timestamp","order_approved_at",
        "order_delivered_carrier_date","order_delivered_customer_date",
        "order_estimated_delivery_date"
    ]
    for c in ts_cols_orders:
        if c in orders.columns:
            orders[c] = pd.to_datetime(orders[c], errors="coerce")
    if "shipping_limit_date" in order_items.columns:
        order_items["shipping_limit_date"] = pd.to_datetime(order_items["shipping_limit_date"], errors="coerce")

    # numerics
    for col in ["product_weight_g","product_length_cm","product_height_cm","product_width_cm"]:
        if col in products.columns:
            products[col] = pd.to_numeric(products[col], errors="coerce")
    for col in ["price","freight_value"]:
        if col in order_items.columns:
            order_items[col] = pd.to_numeric(order_items[col], errors="coerce")
    for col in ["payment_sequential","payment_installments","payment_value"]:
        if col in payments.columns:
            payments[col] = pd.to_numeric(payments[col], errors="coerce")

    # curate
    dim_customers = customers[["customer_id","customer_unique_id","customer_city","customer_state"]].drop_duplicates()
    dim_orders = orders[[
        "order_id","order_status","order_purchase_timestamp","order_approved_at",
        "order_delivered_carrier_date","order_delivered_customer_date","order_estimated_delivery_date"
    ]].drop_duplicates()
    dim_products = products[[
        "product_id","product_category_name","product_weight_g","product_length_cm","product_height_cm","product_width_cm"
    ]].drop_duplicates()
    fact_order_items = order_items[[
        "order_id","product_id","seller_id","shipping_limit_date","price","freight_value"
    ]].copy()
    fact_payments = payments[[
        "order_id","payment_sequential","payment_type","payment_installments","payment_value"
    ]].copy()

    # write parquet and return paths (small XCom)
    outputs = {
        "dim_customers": PROCESSED / "dim_customers.parquet",
        "dim_orders": PROCESSED / "dim_orders.parquet",
        "dim_products": PROCESSED / "dim_products.parquet",
        "fact_order_items": PROCESSED / "fact_order_items.parquet",
        "fact_payments": PROCESSED / "fact_payments.parquet",
    }
    dim_customers.to_parquet(outputs["dim_customers"], index=False)
    dim_orders.to_parquet(outputs["dim_orders"], index=False)
    dim_products.to_parquet(outputs["dim_products"], index=False)
    fact_order_items.to_parquet(outputs["fact_order_items"], index=False)
    fact_payments.to_parquet(outputs["fact_payments"], index=False)

    # return string paths for XCom
    return {k: str(v) for k, v in outputs.items()}
