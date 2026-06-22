{{
    config(
        materialized='view'
    )
}}

SELECT
    invoice_no,
    stock_code,
    TRIM(description)                       AS description,
    quantity,
    invoice_date,
    price,
    customer_id,
    country,
    run_date,
    CAST(invoice_date AS DATE)              AS invoice_date_day,
    ROUND(quantity * price, 2)              AS line_total_gbp
FROM {{ source('raw', 'raw_orders') }}
WHERE
    quantity > 0
    AND price  > 0
    AND customer_id IS NOT NULL
