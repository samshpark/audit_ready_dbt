{% test accepted_values(model, column_name, values, quote=true) %}
{#- Project override of dbt-core built-in accepted_values test: the
   default macro selects `from {{ model }}` with no alias, which is fine
   on DuckDB but ambiguous on BigQuery whenever a column shares its name
   with the model (e.g. order_reconciliation.order_reconciliation) --
   BigQuery then resolves the unqualified column as the whole row STRUCT.
   Aliasing the source avoids that collision on every adapter. -#}

with all_values as (

    select
        _accepted_values_source.{{ column_name }} as value_field,
        count(*) as n_records

    from {{ model }} as _accepted_values_source
    group by _accepted_values_source.{{ column_name }}

)

select *
from all_values
where value_field not in (
    {% for value in values -%}
        {% if quote -%}
        '{{ value }}'
        {%- else -%}
        {{ value }}
        {%- endif -%}
        {%- if not loop.last -%},{%- endif %}
    {%- endfor %}
)

{% endtest %}
