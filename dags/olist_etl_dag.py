from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

from src.extract import extract_olist
from src.transform import transform_olist
from src.load import load_to_dw

with DAG(
    dag_id="olist_etl",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["olist","ecommerce","etl"],
) as dag:

    def _extract(ti):
        payload = extract_olist()
        for k, v in payload.items():
            ti.xcom_push(key=k, value=v)

    extract = PythonOperator(
        task_id="extract",
        python_callable=_extract
    )

    def _transform(ti):
        out = transform_olist(
            ti.xcom_pull(task_ids="extract", key="customers"),
            ti.xcom_pull(task_ids="extract", key="orders"),
            ti.xcom_pull(task_ids="extract", key="order_items"),
            ti.xcom_pull(task_ids="extract", key="products"),
            ti.xcom_pull(task_ids="extract", key="payments"),
        )
        for k, v in out.items():
            ti.xcom_push(key=k, value=v)

    transform = PythonOperator(
        task_id="transform",
        python_callable=_transform
    )

    def _load(ti):
        return load_to_dw(
            ti.xcom_pull(task_ids="transform", key="dim_customers"),
            ti.xcom_pull(task_ids="transform", key="dim_orders"),
            ti.xcom_pull(task_ids="transform", key="dim_products"),
            ti.xcom_pull(task_ids="transform", key="fact_order_items"),
            ti.xcom_pull(task_ids="transform", key="fact_payments"),
        )

    load = PythonOperator(
        task_id="load",
        python_callable=_load
    )

    extract >> transform >> load
