# BigQuery `prod` Target — Build Verification

Evidence that the models compile and run correctly against the live BigQuery
warehouse, not just DuckDB — supporting the Multi-Environment claim in the
[architecture doc](architecture.md#2-high-performance-local-development).

- **Date**: 2026-09-12
- **Command**: `dbt build --target prod`
- **dbt version**: 1.10.20 (`dbt-bigquery`)
- **Elapsed time**: 119.5s
- **Raw output**: [`bigquery_prod_run_results.raw.json`](bigquery_prod_run_results.raw.json) — full dbt `run_results.json` artifact (193 nodes), GCP project ID and local paths masked

## Result

| Status | Count |
|---|---|
| pass | 166 |
| success | 22 |
| warn | 1 |
| no-op | 4 |
| **error** | **0** |

The single `warn` is the expected `assert_fulfillment_lead_time_within_baseline`
test (see [Quality Control](architecture.md#6-quality-control)) — a known source-data defect the test is designed to
flag, not a build failure.

Zero errors across every model, snapshot, seed, and test confirms the
cross-database macros (`dbt.type_float()`, `dbt.datediff()`, `dbt.date_trunc()`,
`dbt_utils.date_spine()`, `within_incremental_lookback()`) compile and execute
correctly on BigQuery, not just DuckDB.

## Build times (models, seed, snapshot)

The 167 individual data tests are omitted below for brevity — all passed
except the one warn noted above.

| Name | Kind | Status | Time (s) |
|---|---|---|---|
| scd_products | snapshot | success | 17.36 |
| order_reconciliation | model | success | 7.12 |
| revenue | model | success | 7.06 |
| refund_reconciliation | model | success | 6.97 |
| audit_materiality_thresholds | seed | success | 6.94 |
| order_item_revenue | model | success | 6.93 |
| inventory_fiscal_report | model | success | 6.35 |
| metricflow_time_spine | model | success | 6.11 |
| inventory_sellthrough | model | success | 3.86 |
| stg_thelook_ecommerce__users | model | success | 2.80 |
| stg_thelook_ecommerce__orders | model | success | 2.13 |
| stg_incremental__order_items | model | success | 2.10 |
| stg_thelook_ecommerce__products | model | success | 2.04 |
| stg_incremental__orders | model | success | 1.91 |
| stg_thelook_ecommerce__inventory_items | model | success | 1.87 |
| stg_incremental__inventory_items | model | success | 1.84 |
| stg_thelook_ecommerce__order_items | model | success | 1.80 |
| int_order_items_aggregated | model | success | 1.78 |
| int_orders_joined | model | success | 1.65 |
| int_inventory_items_joined | model | success | 1.62 |
| int_order_items_unioned | model | success | 1.55 |
| int_inventory_items_unioned | model | success | 1.49 |
