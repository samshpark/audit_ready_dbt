{% macro duckdb__type_float() %}
    {#- dbt's default type_float() renders `float`, which DuckDB treats as
       32-bit REAL -- too narrow for currency (a unit cost of 44.475000 becomes
       44.474998 and then rounds to the wrong cent). BigQuery FLOAT64, Snowflake
       FLOAT and Athena double are all 64-bit, so DuckDB is aligned to double. -#}
    double
{% endmacro %}

{% macro set_utc_session_timezone() %}
    {#- Source timestamps are UTC (TIMESTAMPTZ in DuckDB), and DuckDB converts
       them to the session time zone -- the host machine's -- when staging casts
       them to TIMESTAMP. Pinning UTC keeps posting dates and month-end cut-off
       identical on every machine and every warehouse. Other adapters already
       treat these timestamps as UTC, so the hook renders nothing for them. -#}
    {%- if target.type == 'duckdb' -%}
        set global TimeZone = 'UTC'
    {%- endif -%}
{% endmacro %}
