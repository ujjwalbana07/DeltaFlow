# DeltaFlow Commerce

End-to-end data engineering pipeline on a retail dataset: incremental ingestion
from PostgreSQL into a Databricks Delta Lake, dbt transformations, and Airflow orchestration.


PostgreSQL (Neon) -> Databricks (bronze) -> dbt (silver, gold) -> Airflow schedules everything

## Tech stack
| Layer | Tool |
|---|---|
| Source database | PostgreSQL (Neon) |
| Lakehouse | Databricks, Delta Lake |
| Transformations | dbt Core (dbt-databricks) |
| Orchestration | Apache Airflow (Docker) |
| Language | Python, SQL |

## Data model
- Source tables: customers, stores, products, employees, orders, order_items
- Bronze: raw copy of source tables
- Silver technical: cleaned incremental models (`*_t`)
- Silver business: one big table (`obt_b`)
- Gold: dimensions and facts

## Features
- Incremental loads using MERGE on a unique key
- dbt tests (unique, not_null, relationships)
- Reusable Jinja macros and a metadata-driven OBT build
- Single Airflow DAG: ingest, transform, test

## Setup
1. Clone the repo
2. Create a virtual environment: `uv sync`
3. Copy `.env.example` to `.env` and fill in your own values
4. Add your Databricks connection to `~/.dbt/profiles.yml`
5. Run: `dbt build`

## Notes
Synthetic retail dataset, not real company data.
Built while following a public data engineering tutorial, with my own changes
(Neon instead of the tutorial's database, and ...).
