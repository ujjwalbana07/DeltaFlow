# Pipeline validation — November 3, 2026

Verified the full orchestration DAG in the local development environment.

- DAG state: success; all 10 tasks succeeded.
- Databricks ingestion job configured through DATABRICKS_JOB_ID: completed successfully.
- dbt tests: 4 technical silver, 3 business silver, 6 gold; all passed.
- All seven Docker services healthy.
- Read-only aggregate warehouse checks passed.

| Output | Rows |
|---|---:|
| Silver orders | 10,000 |
| Silver order items | 30,021 |
| Business OBT | 30,021 |
| Gold order-item facts | 30,021 |
| Order dimension | 30,021 |
| Customer dimension | 1,991 |
| Product dimension | 500 |
| Store dimension | 25 |
| Employee dimension | 250 |

Fact duplicate order-item keys: 0. Fact null relationship keys: 0. Duplicate current keys in each of the five snapshots: 0. Customer/product/store/order dimensions follow the reference's order-derived models; customers without orders are not included in the customer dimension.

The initial execution exposed a store-to-employee join that inflated facts to 300,513 rows. Removed that join, removed unsupported employee attribution from order facts, sourced the employee dimension directly from silver employees, and added business/gold uniqueness tests. Rebuilt affected outputs, then verified the full DAG again.

Source freshness thresholds remain unconfigured. Execution is on demand (`schedule=None`); this verification did not enable a recurring schedule. At order-item grain, use line_amount for item-level revenue; total_amount repeats the order total across its items.

Repeat the aggregate checks with the command in README.md. These checks validate keys, counts, connectivity, and execution; business rules beyond the available source fields remain outside this validation.
