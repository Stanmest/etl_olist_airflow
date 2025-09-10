import pandas as pd
from pathlib import Path
RAW = Path("/opt/airflow/data/raw")
FILES = {
    "customers": "olist_customers_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "products": "olist_products_dataset.csv",
    "payments": "olist_order_payments_dataset.csv"
}
def extract_olist() -> dict:
    dfs = {}
    for k, fn in FILES.items():
        path = RAW / fn
        if not path.exists():
            raise FileNotFoundError(f"Missing file: {path}")
        dfs[k] = pd.read_csv(path)
    return {k: v.to_json(orient="records") for k, v in dfs.items()}
