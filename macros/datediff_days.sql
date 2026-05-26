{% macro datediff_days(start_col, end_col) %}
    DATEDIFF('day', CAST({{ start_col }} AS DATE), CAST({{ end_col }} AS DATE))
{% endmacro %}
