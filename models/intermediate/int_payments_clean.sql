-- Build interim data layer at payment level by cleaning and aggregating payment details.

WITH payments AS (
    SELECT
        payment_id,
        order_id,
        amount,
        payment_method
    FROM {{ ref('stg_payments') }}
),
orders AS (
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

)
    SELECT
        p.payment_id,
        p.order_id,
        p.amount AS payment_amount,
        p.payment_method,
        o.order_date,
        o.order_status,
        o.customer_id,
        c.first_name,
        c.last_name
    FROM payments p
    LEFT JOIN orders o 
        ON p.order_id = o.order_id
    LEFT JOIN customers c 
        ON o.customer_id = c.customer_id
    WHERE p.amount > 0