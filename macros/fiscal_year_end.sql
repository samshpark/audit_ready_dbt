{% macro fiscal_year_end(year_col) %}
    CAST({{ year_col }} || '-12-31' AS DATE)
{% endmacro %}
