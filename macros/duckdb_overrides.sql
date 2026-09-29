{% macro duckdb__type_float() %}
    {#- dbt's default type_float() renders `float`, which DuckDB treats as
       32-bit REAL -- too narrow for currency (a unit cost of 44.475000 becomes
       44.474998 and then rounds to the wrong cent). BigQuery FLOAT64, Snowflake
       FLOAT and Athena double are all 64-bit, so DuckDB is aligned to double. -#}
    double
{% endmacro %}

{% macro duckdb__alter_column_type(relation, column_name, new_column_type) %}
    {#- dbt's default changes a column type in four statements (add temp
       column, copy, drop, rename), which an incremental model with
       on_schema_change='sync_all_columns' then follows with a MERGE in the
       same transaction. DuckDB rejects that commit ("another transaction has
       altered this table"), so an existing incremental table could never have
       a column type changed in place. DuckDB supports the change as a single
       ALTER COLUMN ... TYPE, which commits cleanly alongside the MERGE. -#}
    {% call statement('alter_column_type') %}
        alter table {{ relation.render() }}
        alter column {{ adapter.quote(column_name) }} type {{ new_column_type }}
    {% endcall %}
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
