{% macro string_agg_distinct(column_name) %}
    {#- DuckDB/BigQuery's string_agg(distinct col order by col) syntax has no
       Snowflake equivalent -- Snowflake requires listagg(distinct col, sep)
       within group (order by col) instead. -#}
    {%- if target.type == 'snowflake' -%}
        listagg(distinct {{ column_name }}, ', ') within group (order by {{ column_name }})
    {%- else -%}
        string_agg(distinct {{ column_name }} order by {{ column_name }})
    {%- endif -%}
{% endmacro %}
