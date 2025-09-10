import io, pandas as pd
def _from_xcom(json_str):
    return pd.read_json(io.StringIO(json_str), orient="records")
def transform_olist(customers_json, orders_json, order_items_json, products_json, payments_json):
    customers = _from_xcom(customers_json); orders = _from_xcom(orders_json)
    order_items = _from_xcom(order_items_json); products = _from_xcom(products_json)
    payments = _from_xcom(payments_json)
    for df in [customers, orders, order_items, products, payments]:
        df.columns = [c.strip().lower() for c in df.columns]
    dim_customers = customers[["customer_id","customer_unique_id","customer_city","customer_state"]].drop_duplicates()
    dim_orders = orders[["order_id","order_status","order_purchase_timestamp","order_approved_at",
                         "order_delivered_carrier_date","order_delivered_customer_date","order_estimated_delivery_date"]].drop_duplicates()
    dim_products = products[["product_id","product_category_name","product_weight_g","product_length_cm","product_height_cm","product_width_cm"]].drop_duplicates()
    fact_order_items = order_items[["order_id","product_id","seller_id","shipping_limit_date","price","freight_value"]].copy()
    fact_payments = payments[["order_id","payment_sequential","payment_type","payment_installments","payment_value"]].copy()
    out = {}
    for name, df in {"dim_customers":dim_customers,"dim_orders":dim_orders,"dim_products":dim_products,
                     "fact_order_items":fact_order_items,"fact_payments":fact_payments}.items():
        out[name] = df.to_json(orient="records")
    return out
