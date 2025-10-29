{% test compare_order_amount(model, column_name, compare_model, compare_column_name, tolerance=0.0) %}

with left_sum as (
  select coalesce(sum({{ column_name }}), 0) as total_left
  from {{ model }}
),
right_sum as (
  select coalesce(sum({{ compare_column_name }}), 0) as total_right
  from {{ compare_model }}
)
select
  total_left,
  total_right,
  (total_left - total_right) as difference
from left_sum, right_sum
where abs(total_left - total_right) > {{ tolerance }}

{% endtest %}
