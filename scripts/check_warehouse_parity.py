"""
Cross-Warehouse Parity Check

Purpose:
    Prove the books agree to the cent on every warehouse. Runs one metrics
    query per dbt target via `dbt show`, so ref() resolves to each target's own
    relations and no warehouse-specific SQL or client is needed, then compares
    every metric against the first target.

Metrics:
    Journal-entry debits by entry type, journal line and entry counts, and
    headline mart totals (recognized revenue, refunds, COGS, reconciliation
    breaks, cut-off risk orders).

Usage:
    python scripts/check_warehouse_parity.py --targets parity_duckdb prod athena
    (run from the dbt project root; targets must already be built from the same sources)
"""

import argparse
import sys
from decimal import Decimal

from dbt.cli.main import dbtRunner

TOLERANCE = Decimal("0.01")

# Tables are aliased: BigQuery resolves an unqualified name that matches the
# table (order_reconciliation.order_reconciliation) as the whole row STRUCT.
# Values are cast to numeric, not float -- dbt.type_float() is 32-bit REAL on
# DuckDB, which cannot hold cents at these magnitudes.
METRICS_SQL = """
{% set n = dbt.type_numeric() %}
with je as (
    select t.entry_type, sum(t.debit_amount) as debits
    from {{ ref('journal_entries') }} as t
    group by t.entry_type
)
select 'journal_entries.' || je.entry_type || '.debits' as metric, cast(round(je.debits, 2) as {{ n }}) as value
from je
union all
select 'journal_entries.rows', cast(count(*) as {{ n }}) from {{ ref('journal_entries') }} as t
union all
select 'journal_entry_lines.rows', cast(count(*) as {{ n }}) from {{ ref('journal_entry_lines') }} as t
union all
select 'revenue.recognized_revenue', cast(round(sum(t.recognized_revenue), 2) as {{ n }})
from {{ ref('revenue') }} as t
union all
select 'revenue.cutoff_risk_orders', cast(count(*) as {{ n }})
from {{ ref('revenue') }} as t where t.cutoff_status = 'POTENTIAL CUT-OFF RISK'
union all
select 'refund_reconciliation.refund_amount', cast(round(sum(t.refund_amount), 2) as {{ n }})
from {{ ref('refund_reconciliation') }} as t
union all
select 'order_reconciliation.breaks', cast(count(*) as {{ n }})
from {{ ref('order_reconciliation') }} as t where t.order_reconciliation <> 'RECONCILIATION SUCCESSFUL'
union all
select 'inventory_fiscal_report.period_cogs', cast(round(sum(t.period_cogs_amount), 2) as {{ n }})
from {{ ref('inventory_fiscal_report') }} as t
"""


def fetch_metrics(runner: dbtRunner, target: str) -> dict[str, Decimal]:
    res = runner.invoke(["--log-level", "none", "show", "--inline", METRICS_SQL, "--target", target, "--limit", "-1"])
    if not res.success:
        raise RuntimeError(f"dbt show failed on target '{target}': {res.exception}")
    table = res.result.results[0].agate_table
    return {row["metric"]: Decimal(str(row["value"])).quantize(TOLERANCE) for row in table.rows}


def compare(metrics_by_target: dict[str, dict[str, Decimal]]) -> list[str]:
    """Return one message per metric that is missing or differs from the reference target."""
    reference_target, reference = next(iter(metrics_by_target.items()))
    problems = []
    for target, metrics in metrics_by_target.items():
        for name in sorted(set(reference) | set(metrics)):
            expected, actual = reference.get(name), metrics.get(name)
            if expected is None or actual is None:
                problems.append(f"{name}: missing on {reference_target if expected is None else target}")
            elif abs(expected - actual) > TOLERANCE:
                problems.append(f"{name}: {reference_target}={expected} vs {target}={actual}")
    return problems


def run_parity(targets: list[str]) -> dict[str, dict[str, Decimal]]:
    runner = dbtRunner()
    metrics_by_target = {t: fetch_metrics(runner, t) for t in targets}

    names = sorted(set().union(*metrics_by_target.values()))
    width = max(len(n) for n in names)
    print(f"{'metric':<{width}}  " + "  ".join(f"{t:>16}" for t in targets))
    for name in names:
        values = [metrics_by_target[t].get(name) for t in targets]
        print(f"{name:<{width}}  " + "  ".join(f"{str(v):>16}" for v in values))

    problems = compare(metrics_by_target)
    if problems:
        raise AssertionError("Warehouse parity failed:\n  " + "\n  ".join(problems))
    print(f"\nParity OK: {len(names)} metrics match across {', '.join(targets)}")
    return metrics_by_target


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--targets", nargs="+", required=True, help="dbt targets to compare; first is reference")
    args = parser.parse_args()
    try:
        run_parity(args.targets)
        return 0
    except (AssertionError, RuntimeError) as e:
        print(e, file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
