{% macro fiscal_year_end(year_col) %}
    LEAST(CAST({{ year_col }} || '-12-31' AS DATE), current_date)
{% endmacro %}
