# Olist E-commerce Analytics Pipeline (Airflow + Postgres + dbt)

End-to-end, reproducible analytics stack built on the real **Olist Brazilian E-commerce** dataset.

- **Orchestration:** Apache **Airflow** (Docker)
- **Warehouse:** **Postgres 16** (`olist` schema)
- **Transforms:** **pandas** → Parquet handoff → Postgres **COPY** (fast & reliable)
- **Modeling:** **dbt** (staging, marts, tests, docs & lineage)

> Why it matters: the pipeline is **idempotent** (safe to re-run), scales beyond XCom limits, and produces tested, documented models a BI team can use immediately.

## Screenshots


### Airflow DAG (end-to-end success)
![Airflow DAG](assets/airflow_dag.jpeg)

### dbt Lineage – Overview
![dbt lineage overview](assets/dbt_lineage_overview.jpeg)

### dbt Lineage – Fact build path
![dbt lineage fact enriched](assets/dbt_lineage_fact_enriched.jpeg)

### dbt Lineage – Dimension example
![dbt lineage dim product](assets/dbt_lineage_dim_product.jpeg)

