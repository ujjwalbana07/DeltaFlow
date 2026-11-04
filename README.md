# DeltaFlow Commerce

End-to-end data engineering pipeline on a retail dataset: incremental ingestion
from PostgreSQL into a Databricks Delta Lake, dbt transformations, and Airflow orchestration.


PostgreSQL (Neon) -> Databricks (bronze) -> dbt (silver, gold) -> Airflow orchestrates the pipeline

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
- dbt tests for key uniqueness, required values, and missing relationships
- Reusable Jinja macros and a metadata-driven OBT build
- Single Airflow DAG: ingest, transform, test

## Project structure

```text
Airflow/
  config/airflow.cfg
  dags/orchestrate.py
  logs/
  plugins/
  Dockerfile
  requirements.txt
  docker-compose.yaml
  .env.example
  .env                 # private local settings; ignored by Git
walmart_project/
  dbt_project.yml
  profiles.yml         # environment variable references only
  models/
  snapshots/
  tests/
```

The dbt project remains at the workspace root. Compose mounts it at `/opt/airflow/walmart_project`, matching the DAG. Reference files remain untouched.

## Start locally

1. Start Docker Desktop.
2. Copy `Airflow/.env.example` to `Airflow/.env` and fill in your Databricks host, warehouse HTTP path, token, catalog, schema, and ingestion job ID. Generate a Fernet key and JWT secret for this checkout. The upstream ingestion job must already exist in your Databricks workspace; its implementation is managed outside this repository.
3. Run from the workspace root:

```sh
cd Airflow
docker compose config --quiet
docker compose build
docker compose up airflow-init
docker compose up -d
```

Open http://localhost:8080. The local development login defaults to `airflow` / `airflow`. Trigger the `orchestrate` DAG manually; it has no automatic schedule and permits one active run at a time.

```sh
# Check for DAG import errors
docker compose run --rm airflow-cli airflow dags list-import-errors
# Check warehouse credentials and connectivity
docker compose run --rm airflow-cli dbt debug --project-dir /opt/airflow/walmart_project
# View service status and stop services without removing database data
docker compose ps
docker compose down
```

The Databricks token must have access to the ingestion job and SQL warehouse, permission to read bronze tables, and permission to create/write silver and gold schemas in `DATABRICKS_CATALOG`. Source tables must include the columns referenced by the models, including `updated_timestamp`.

Dimension snapshots consume ephemeral models directly; no separate run is needed for ephemeral models. Order snapshots use `order_item_id`, matching the order-item grain. Employee snapshots use employee/store pairs. Orders do not contain employee IDs. The employee/store join was removed after validation found it multiplied 30,021 order items into 300,513 fact rows. Employees are snapshotted directly from employees_t; fact_orders has no employee_id. The fact grain is one row per order item, enforced by dbt tests. total_amount is the order total repeated on its items; sum line_amount for item-level revenue, or aggregate total_amount once per order.

Source freshness currently has no configured timestamp thresholds, so that task does not enforce a freshness SLA. Add source freshness configuration once ingestion latency requirements are known.

## Validation

Docker Compose configuration and dbt parsing were validated locally. The Docker image builds successfully, services pass health checks, warehouse connectivity succeeds, and the repaired Airflow DAG completed all 10 tasks successfully on November 3, 2026. All 13 dbt tests passed. The final fact table contains 30,021 rows with zero duplicate order-item keys and zero null relationship keys; all five snapshots have zero duplicate current keys. Aggregate validation found and corrected an employee join that duplicated order-item facts. Gold key tests now run as the final DAG task.

The Dockerfile pins the Airflow version while installing dependencies, following [Airflow image guidance](https://airflow.apache.org/docs/docker-stack/build.html). Snapshots follow [dbt's ephemeral relation guidance](https://docs.getdbt.com/docs/build/snapshots).

Read-only aggregate checks can be rerun from the workspace root:

```sh
docker compose -f Airflow/docker-compose.yaml exec -T airflow-worker python < scripts/check_warehouse.py
```

The project uses a synthetic retail dataset.
