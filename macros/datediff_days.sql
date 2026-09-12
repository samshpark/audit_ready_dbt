{% macro datediff_days(start_col, end_col) %}
    {{ dbt.datediff(
        "cast(" ~ start_col ~ " as date)",
        "cast(" ~ end_col ~ " as date)",
        "day"
    ) }}
{% endmacro %}
