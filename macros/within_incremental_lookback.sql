{% macro within_incremental_lookback(column_name) %}
    {#- `current_timestamp - interval 'N days'` (quoted interval literal) is
       DuckDB/Postgres syntax; BigQuery rejects the quoted form and also
       rejects negative intervals in DATETIME_ADD. Restated as
       `column + N days >= current_timestamp`, which is algebraically
       equivalent and only needs a positive dbt.dateadd(). -#}
    {#- dbt.dateadd() returns DATETIME on BigQuery, which cannot be compared
       directly against TIMESTAMP -- cast back so the comparison type-checks
       on every adapter. -#}
    cast(
        {{ dbt.dateadd('day', var("incremental_lookback_days"), column_name) }}
        as timestamp
    ) >= current_timestamp
{% endmacro %}
