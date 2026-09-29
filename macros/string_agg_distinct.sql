{% macro string_agg_distinct(column_name) %}
    {#- DuckDB/BigQuery's string_agg(distinct col order by col) syntax has no
       Snowflake equivalent -- Snowflake requires listagg(distinct col, sep)
       within group (order by col) instead. Athena (Trino) has neither form;
       array_agg + array_distinct + array_sort + array_join is its equivalent. -#}
    {%- if target.type == 'snowflake' -%}
        listagg(distinct {{ column_name }}, ', ') within group (order by {{ column_name }})
    {%- elif target.type == 'athena' -%}
        array_join(array_sort(array_distinct(array_agg({{ column_name }}))), ',')
    {%- else -%}
        string_agg(distinct {{ column_name }} order by {{ column_name }})
    {%- endif -%}
{% endmacro %}
