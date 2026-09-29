{% macro fiscal_year_end(year_col) %}
    {#- Cast the year to string before concatenating: Athena (Trino) rejects
       `bigint || varchar`, which other adapters coerce implicitly. -#}
    LEAST(CAST(CAST({{ year_col }} AS {{ dbt.type_string() }}) || '-12-31' AS DATE), current_date)
{% endmacro %}
