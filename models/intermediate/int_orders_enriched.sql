-- Build interim data layer at order level by enriching orders with customer and payment details.

WITH orders AS (
    SELECT
        order_id,
        customer_id,
        order_date,
        order_status
    FROM {{ ref('stg_orders') }}
),
customers AS (
    SELECT
        customer_id,
        first_name,
        last_name
    FROM {{ ref('stg_customers') }}
), 
payments AS (
    SELECT
        order_id,
        SUM(amount) AS total_amount,
        COUNT(DISTINCT payment_id) AS payment_count,
        STRING_AGG(COALESCE(payment_method,'Unkown payment method'), '-') AS payment_methods
    FROM {{ ref('stg_payments') }}
    GROUP BY order_id
)
SELECT
    o.order_id,
    o.order_date,
    o.order_status,
    c.customer_id,
    row_number() over (
                        partition by c.customer_id
                 order by o.order_date asc, o.order_id asc  -- tie-breaker!
                 ) as order_sequence_per_customer,

    (case when row_number() over (
        partition by c.customer_id
            order by o.order_date asc, o.order_id asc
            ) = 1 then true else false end) as is_first_order,
    c.first_name,
    c.last_name,
    p.total_amount,
    p.payment_count,
    p.payment_methods
FROM orders o
LEFT JOIN customers c 
    ON o.customer_id = c.customer_id
LEFT JOIN payments p 
    ON o.order_id = p.order_id