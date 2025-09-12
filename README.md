# Olist E-commerce Analytics Pipeline (Airflow + Postgres + dbt)

This is an end-to-end, **production-style** analytics stack on the real **Olist Brazilian E-commerce** dataset.  
It extracts CSVs, transforms them with **pandas**, bulk-loads into **Postgres** using **COPY**, and models the warehouse with **dbt** (tests + lineage + docs).

- **Orchestration:** Apache **Airflow** (Docker)
- **Warehouse:** **Postgres 16** (`olist` schema)
- **Transforms:** **pandas** → **Parquet** handoff → **COPY** to Postgres (fast & reliable)
- **Modeling:** **dbt** (staging, marts, tests, docs & lineage)

> **Why this matters:** Idempotent (safe to re-run), avoids XCom size limits via Parquet handoff, and produces tested, documented models a BI team can use immediately.

---

## ✨ What I Did

- Set up a **Dockerized** stack (Airflow webserver/scheduler + Postgres DW).
- Built an **Airflow DAG** (`extract → transform → load`) with:
  - **Extract:** ingest 5 Olist CSVs (customers, orders, items, payments, products).
  - **Transform:** strong typing + cleaning in **pandas**; write **Parquet** to `data/processed/`.
  - **Load:** fast, reliable **COPY** into Postgres; **UPSERT** dimensions; **TRUNCATE + COPY** facts.
- Solved tricky ingestion issues (pandas/SQLAlchemy `.to_sql` pitfalls) by switching to **COPY**.
- Implemented **idempotent** behavior so reruns don’t fail on unique constraints.
- Modeled the warehouse with **dbt**:
  - **sources → staging → marts** (dims & facts), plus schema/data **tests**.
  - Generated **docs** and **lineage**; added an **exposure** for analytics.
- Added CLI verification steps (row counts, DAG run states) for quick health checks.
- Wrote a recruiter-friendly **README** with architecture, screenshots, and quickstart.

---

## 🔎 Highlights

- **Reliable loads:** Dimensions **UPSERTed**; facts **TRUNCATE + COPY** → repeatable runs.
- **Scales cleanly:** Large payloads don’t go through XCom; tasks pass **file paths** to Parquet.
- **Fast ingestion:** Uses Postgres **COPY** (psycopg2) instead of `DataFrame.to_sql`.
- **Quality & docs:** dbt tests pass; lineage + exposures documented.

---

## 📸 Screenshots

> Files expected under `assets/`. These names match what you already have.

### Airflow DAG (end-to-end)
![Airflow DAG](assets/airflow_dag.jpeg)

### dbt Lineage – Overview
![dbt lineage overview](assets/dbt_lineage_overview.jpeg)

### dbt Lineage – Fact build path
![dbt lineage fact enriched](assets/dbt_lineage_fact_enriched.jpeg)

### dbt Lineage – Dimension example
![dbt lineage dim product](assets/dbt_lineage_dim_product.jpeg)

---

## 🏗️ Architecture

```mermaid
flowchart TD
  A["Olist CSVs (Kaggle)"]
  B["Airflow (orchestration)"]
  C["Transform (pandas) to Parquet"]
  D["Postgres (schema: olist)"]
  E["dbt staging (views)"]
  F["dbt marts (tables: dims and facts)"]
  G["Docs and Exposure (Lineage, Dashboard)"]

  A -->|Extract| B
  B --> C
  C --> D
  D -->|sources| E
  E --> F
  F --> G
