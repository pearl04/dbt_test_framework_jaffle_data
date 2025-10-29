-- Build a fact table at order level. This will be used for looking at orders over time.

SELECT
    order_id,
    order_date,
    order_status,
    customer_id,
    total_amount AS order_total_amount,
    payment_count AS total_payments_made,
    payment_methods AS payment_methods_used
FROM {{ ref('int_orders_enriched') }}