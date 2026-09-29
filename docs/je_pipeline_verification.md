# Journal-Entry Pipeline — Verification

Evidence that the GL layer (dbt `journal_entries` + the `audit-ready-je-pipeline`
Lambda) is deployed, posts amounts that tie to the marts, and runs end to end
from Airflow. Backs README [§4.5](../README.md#5-general-ledger-posting--exception-pipeline-aws-lambda).

- **Date**: 2026-09-29
- **Deployed with**: `sam build && sam deploy` from `lambdas/`
  ([`template.yaml`](../lambdas/template.yaml), [`samconfig.toml`](../lambdas/samconfig.toml))
- **Stack**: `audit-ready-je-pipeline` (us-east-1) — Lambda (Python 3.12, arm64),
  execution role, versioned reports bucket, 30-day log group

## 1. dbt: journal entries tie to the marts

`journal_entries`, `journal_entry_lines`, the two seeds, and their tests pass
on DuckDB, BigQuery, and Athena (224 pass / 0 error per full build). Two
singular tests carry the accounting controls:

- `assert_journal_entries_balanced` — debits = credits for every entry
- `assert_journal_entries_reconcile_to_marts` — posted revenue ties to the
  item-level subledger, returns to `refund_reconciliation`, COGS to
  `inventory_fiscal_report` (0.01 tolerance; 5.00 for COGS, which that report
  rounds per product-year)

Totals are also identical across the three warehouses — see
[`cross_warehouse_parity.md`](cross_warehouse_parity.md).

**Independent re-computation.** Before the GL moved into dbt, the same August
2026 entries were produced by a Python implementation reading the marts
directly. The dbt version reproduces it exactly:

| August 2026 | Python prototype | dbt `journal_entries` | Direct mart query |
|---|---|---|---|
| Revenue (REV) | 125,331.00 | 125,331.00 | 125,331.00 (`revenue`, shipped in Aug) |
| Returns (RET) | 30,057.00 | 30,057.00 | 30,057.00 (`order_item_revenue`, returned in Aug) |
| COGS | 25,198.16 | 25,198.16 | — |
| Journal entries | 93 | 93 | — |

## 2. Lambda: direct invocation

```bash
aws lambda invoke --function-name audit-ready-je-pipeline --cli-binary-format raw-in-base64-out \
  --payload '{"start_date":"2026-08-01","end_date":"2026-08-31"}' out.json
```

StatusCode 200, no `FunctionError`. `run_summary.json`:

```json
{
  "period_start": "2026-08-01",
  "period_end_inclusive": "2026-08-31",
  "journal_entries": 93,
  "net_by_account": {
    "1200 Accounts Receivable": "95274.00",
    "1300 Inventory": "-25198.16",
    "4000 Sales Revenue": "-125331.00",
    "4100 Sales Returns & Allowances": "30057.00",
    "5000 Cost of Goods Sold": "25198.16"
  },
  "exceptions_by_rule": {"CUTOFF_RISK": 26, "FUTURE_DATED_SHIPMENT": 43}
}
```

(The 43 `FUTURE_DATED_SHIPMENT` exceptions were a real defect in the
synthetic-data generator, since resolved — see section 4. After the fix,
the same function run for 2026-09-01..28 reports only `CUTOFF_RISK`.)

Net movement is consistent with double entry: AR 95,274.00 = revenue
125,331.00 − returns 30,057.00, and COGS equals the inventory relief.

Outputs in `s3://audit-ready-dbt-reports-sam2026/je-pipeline/period=2026-08-01_2026-08-31/`:

| File | Size |
|---|---|
| `journal_entries.csv` | 23,635 B |
| `journal_detail.csv` | 80,985 B |
| `exceptions.csv` | 7,049 B |
| `run_summary.json` | 463 B |

## 3. Airflow: end to end

Run `scheduled__2026-09-28T09:00:00+00:00` of `dbt_daily_incremental`, AWS branch:

| Task | Result |
|---|---|
| `upload_incremental_to_s3` | success |
| `dbt_build_athena` | success — 228 nodes, all tests pass |
| `export_journal_entries` | first attempt failed with `AccessDeniedException` (before the Lambda and invoke permission existed); success after deploy |

The first attempt's failure is itself a check of the permission design: the
pipeline's IAM user could not invoke anything until `lambda:InvokeFunction`
was granted on this one function.

## 4. Issues found and fixed during verification

- **Empty period → empty file.** 2026-09-28 had no postings, and the first
  export wrote 0-byte CSVs with no header — indistinguishable from a
  truncated upload on the GL side. Fixed so every file always carries its
  header (the column list is shared by the SQL and the CSV writer). Because
  the bucket is versioned, the original 0-byte export is still retained as a
  prior version of the object.
- **Future-dated shipments — resolved.** The `FUTURE_DATED_SHIPMENT`
  exception rule flagged orders whose `shipped_at` was later than the run
  time: `scripts/generate_daily_incremental.py` wrote shipments, deliveries,
  and returns up to days ahead of generation, so `revenue` recognized revenue
  before shipment. Fixed at the source by making the generator event-driven
  (only events that have already happened are written), and the check was
  moved from the Lambda into an error-severity dbt test,
  `assert_no_future_dated_events`, which fails the Athena build before
  `export_journal_entries` can post. Against the old data the test failed
  with 139 future-dated events; after regenerating, it passes on DuckDB,
  BigQuery, and Athena, and cross-warehouse parity still holds.
