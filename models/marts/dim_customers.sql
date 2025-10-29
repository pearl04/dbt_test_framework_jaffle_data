-- Build dimension data at customer level.

SELECT 
    customer_id,
    first_name,
    last_name,
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(DISTINCT CASE WHEN order_status = 'completed' THEN order_id END) AS completed_orders,
    COUNT(DISTINCT CASE WHEN order_status  LIKE '%return%' THEN order_id END) AS canceled_orders,
    SUM(total_amount) AS lifetime_revenue
FROM {{ ref('int_orders_enriched') }}
GROUP BY customer_id, first_name, last_name