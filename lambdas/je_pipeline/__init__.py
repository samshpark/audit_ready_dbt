"""
Journal-entry export and exception pipeline over the dbt finance marts on Athena.

dbt builds and tests the journal entries (journal_entries, journal_entry_lines).
This package is the integration layer on top:

    handler.py  Lambda entry point: exports the period's entries as a GL upload
                file with support detail, runs the exception rules, writes to S3
    rules.py    exception rules and the Athena datasets they read
    athena.py   boto3-only Athena client (no third-party deps in the Lambda)

Runs as an AWS Lambda invoked by the dbt_daily_incremental DAG, or locally via
`python -m je_pipeline.handler`.
"""
