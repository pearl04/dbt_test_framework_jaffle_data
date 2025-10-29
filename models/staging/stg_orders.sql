-- Cleanup all the orders data
SELECT
    id AS order_id,
    user_id AS customer_id,
    order_date::date AS order_date,
    UPPER(TRIM(status)) AS order_status
    FROM {{ ref('raw_orders') }}