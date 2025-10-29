-- Fetch clean customer data from raw source
SELECT 
    id AS customer_id,
    UPPER(TRIM(first_name)) AS first_name,
    UPPER(TRIM(last_name)) AS last_name,
    FROM {{ ref('raw_customers') }}
