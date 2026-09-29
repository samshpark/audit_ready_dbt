"""
Lambda entry point (and local CLI) for the journal-entry pipeline.

Event (all optional):
    {"start_date": "YYYY-MM-DD", "end_date": "YYYY-MM-DD"}   # end inclusive
Defaults to yesterday (UTC). The dbt_daily_incremental DAG invokes it with the
run's business date after `dbt build --target athena` succeeds.

Journal entries are built and tested in dbt (journal_entries,
journal_entry_lines -- including the debit = credit and control-total tests),
so this function only exports them for GL upload and runs the Python
exception rules (rules.py).

Outputs, under <prefix>/period=<start>_<end>/:
    journal_entries.csv   GL upload file: Dr/Cr lines, one balanced JE per date x type
    journal_detail.csv    order / item level support for every JE amount
    exceptions.csv        items flagged for review
    run_summary.json      period, net movement by account, exception counts

Local usage:
    cd lambdas && AWS_PROFILE=audit-ready-dbt python -m je_pipeline.handler \
        --start 2026-09-01 --end 2026-09-28 --local-dir ../tmp/je_output
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import sys
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

from . import athena, rules
from .athena import FINANCE

ATHENA_OUTPUT = os.environ.get("ATHENA_OUTPUT", "s3://audit-ready-dbt-athena-sam2026/query-results/")
ATHENA_WORKGROUP = os.environ.get("ATHENA_WORKGROUP", "primary")
OUTPUT_BUCKET = os.environ.get("OUTPUT_BUCKET", "")
OUTPUT_PREFIX = os.environ.get("OUTPUT_PREFIX", "je-pipeline")
REGION = os.environ.get("AWS_REGION", "us-east-1")


CENT = Decimal("0.01")

# Export file layouts. Shared by the SQL and the CSV header, so a day with no
# postings still yields a header-only file the GL import can tell from a broken one.
JE_COLUMNS = (
    "je_id", "posting_date", "entry_type", "line_number", "account_code", "account_name",
    "debit_amount", "credit_amount", "support_line_count", "description",
)  # fmt: skip
DETAIL_COLUMNS = ("je_id", "entry_type", "posting_date", "source_ref", "amount")


def export_queries(start: date, end: date) -> dict[str, str]:
    """The period's journal entries and their support lines, exactly as dbt built them."""
    period = f"posting_date >= date '{start.isoformat()}' and posting_date < date '{end.isoformat()}'"
    return {
        "journal_entries": f"""
            select {", ".join(JE_COLUMNS)}
            from {FINANCE}.journal_entries
            where {period}
            order by posting_date, entry_type, line_number
        """,
        "journal_entry_lines": f"""
            select {", ".join(DETAIL_COLUMNS)}
            from {FINANCE}.journal_entry_lines
            where {period}
            order by posting_date, entry_type, source_ref
        """,
    }


def resolve_period(event: dict, today: date) -> tuple[date, date]:
    """Return [start, end) from an inclusive start_date/end_date event, defaulting to yesterday."""
    if not event.get("start_date"):
        return today - timedelta(days=1), today
    start = date.fromisoformat(event["start_date"])
    end_inclusive = date.fromisoformat(event.get("end_date") or event["start_date"])
    if end_inclusive < start:
        raise ValueError(f"end_date {end_inclusive} is before start_date {start}")
    return start, end_inclusive + timedelta(days=1)


def _csv(rows: list[dict], columns: tuple[str, ...]) -> str:
    """Render rows under a fixed header; the header is written even when there are no rows."""
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=columns)
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue()


def to_money(value) -> Decimal:
    return Decimal(str(value)).quantize(CENT, rounding=ROUND_HALF_UP)


def format_money(rows: list[dict], columns: tuple[str, ...]) -> list[dict]:
    """Render amounts with exactly two decimals, as a GL upload file expects."""
    return [{k: str(to_money(v)) if k in columns else v for k, v in row.items()} for row in rows]


def net_by_account(entries: list[dict]) -> dict[str, Decimal]:
    """Debit-positive net movement per account, for the run summary."""
    totals: dict[str, Decimal] = defaultdict(Decimal)
    for line in entries:
        totals[f"{line['account_code']} {line['account_name']}"] += to_money(line["debit_amount"]) - to_money(
            line["credit_amount"]
        )
    return dict(sorted(totals.items()))


def run(athena_client, start: date, end: date, as_of: datetime) -> dict[str, str]:
    """Query, run the exception rules, and render every output file. Returns {filename: content}."""
    data = athena.run_queries(
        athena_client,
        {**export_queries(start, end), **rules.queries(start, end)},
        ATHENA_OUTPUT,
        ATHENA_WORKGROUP,
    )

    entries = data["journal_entries"]
    period_start = datetime.combine(start, datetime.min.time())
    exceptions = rules.run_all(data, period_start)

    summary = {
        "period_start": start.isoformat(),
        "period_end_inclusive": (end - timedelta(days=1)).isoformat(),
        "run_at_utc": as_of.isoformat(timespec="seconds"),
        "journal_entries": len({line["je_id"] for line in entries}),
        "net_by_account": {k: str(v) for k, v in net_by_account(entries).items()},
        "exceptions_by_rule": dict(Counter(e["rule"] for e in exceptions)),
    }
    return {
        "journal_entries.csv": _csv(format_money(entries, ("debit_amount", "credit_amount")), JE_COLUMNS),
        "journal_detail.csv": _csv(format_money(data["journal_entry_lines"], ("amount",)), DETAIL_COLUMNS),
        "exceptions.csv": _csv(exceptions, rules.EXCEPTION_COLUMNS),
        "run_summary.json": json.dumps(summary, indent=2),
    }


def _period_path(start: date, end: date) -> str:
    return f"period={start.isoformat()}_{(end - timedelta(days=1)).isoformat()}"


def write_s3(s3, files: dict[str, str], bucket: str, prefix: str) -> None:
    for name, body in files.items():
        s3.put_object(Bucket=bucket, Key=f"{prefix}/{name}", Body=body.encode("utf-8"))


def lambda_handler(event, context):
    start, end = resolve_period(event or {}, datetime.now(timezone.utc).date())
    as_of = datetime.now(timezone.utc).replace(tzinfo=None)
    session = boto3.Session(region_name=REGION)
    files = run(session.client("athena"), start, end, as_of)
    write_s3(session.client("s3"), files, OUTPUT_BUCKET, f"{OUTPUT_PREFIX}/{_period_path(start, end)}")
    return json.loads(files["run_summary.json"])


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--start", help="Period start date (YYYY-MM-DD); default yesterday")
    parser.add_argument("--end", help="Period end date, inclusive (YYYY-MM-DD); default = start")
    parser.add_argument("--local-dir", help="Write outputs here instead of S3")
    args = parser.parse_args()

    try:
        start, end = resolve_period(
            {"start_date": args.start, "end_date": args.end}, datetime.now(timezone.utc).date()
        )
        as_of = datetime.now(timezone.utc).replace(tzinfo=None)
        session = boto3.Session(region_name=REGION)
        files = run(session.client("athena"), start, end, as_of)
        if args.local_dir:
            out = Path(args.local_dir) / _period_path(start, end)
            out.mkdir(parents=True, exist_ok=True)
            for name, body in files.items():
                (out / name).write_text(body)
            print(f"Wrote {len(files)} files to {out}")
        else:
            if not OUTPUT_BUCKET:
                raise ValueError("Set OUTPUT_BUCKET or pass --local-dir")
            write_s3(session.client("s3"), files, OUTPUT_BUCKET, f"{OUTPUT_PREFIX}/{_period_path(start, end)}")
            print(f"Wrote {len(files)} files to s3://{OUTPUT_BUCKET}/{OUTPUT_PREFIX}/{_period_path(start, end)}")
        print(files["run_summary.json"])
        return 0
    except (ClientError, athena.AthenaQueryError, ValueError) as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
