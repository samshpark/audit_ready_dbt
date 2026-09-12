{{
    config(
        materialized='table',
    )
}}

-- Cross-database date spine: the previous version used DuckDB-only syntax
-- (range()::date), which fails to parse on BigQuery. dbt_utils.date_spine
-- dispatches to each adapter's native date functions instead.
{% set start_of_year = dbt.date_trunc('year', 'current_date') %}
{% set end_date = dbt.dateadd('year', 3, start_of_year) %}

with days as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2019-01-01' as date)",
        end_date=end_date
    ) }}
)

select date_day from days
