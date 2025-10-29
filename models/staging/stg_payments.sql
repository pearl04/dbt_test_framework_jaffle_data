-- Cleanup payments data
SELECT
    id AS payment_id,
    order_id,
    amount::numeric AS amount,
    CASE
        WHEN LOWER(payment_method) LIKE '%coupon%' THEN 'COUPON'
        WHEN LOWER(payment_method) LIKE '%gift%' THEN 'GIFT_CARD'
        WHEN LOWER(payment_method) LIKE '%credit%' THEN 'CREDIT_CARD'
        WHEN LOWER(payment_method) LIKE '%bank%' THEN 'BANK_TRANSFER'
        ELSE 'unknown'
        END AS payment_method
    FROM {{ ref('raw_payments') }}