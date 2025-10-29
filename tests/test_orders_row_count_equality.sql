-- Fails if the int model has more/fewer rows than raw/staging orders
{{ config(tags=['intermediate', 'singular', 'ci']) }}

with orders as (
    select count(*) as n_orders
    from {{ ref('stg_orders') }}
),
int_model as (
    select count(*) as n_rows
    from {{ ref('int_orders_enriched') }}   -- <-- your int model
)
select
  orders.n_orders as expected_orders,
  int_model.n_rows as actual_rows
from orders, int_model
where int_model.n_rows <> orders.n_orders
