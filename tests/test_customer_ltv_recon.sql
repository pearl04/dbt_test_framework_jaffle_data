{{ config(tags=['marts', 'singular', 'ci']) }}

with
-- LTV calculated from the fact table
ltv_from_fct as (
    select
        customer_id,
        sum(order_total_amount) as ltv_calc      
    from {{ ref('fct_orders') }}
    group by 1
),

-- LTV stored in the mart/dimension (if you have it there)
mart_values as (
    select
        customer_id,
        lifetime_revenue as ltv_mart       -- adjust to your column name
    from {{ ref('dim_customers') }}        -- or another mart model
)

-- Fail any mismatch between calc vs stored
select
    coalesce(m.customer_id, f.customer_id) as customer_id,
    m.ltv_mart,
    f.ltv_calc
from mart_values m
full outer join ltv_from_fct f using (customer_id)
where coalesce(m.ltv_mart, 0) <> coalesce(f.ltv_calc, 0)