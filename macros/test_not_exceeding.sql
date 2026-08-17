{% test not_exceeding(model, column_name, other_column) %}

select *
from {{ model }}
where {{ column_name }} > {{ other_column }}

{% endtest %}
