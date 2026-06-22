{{
    config(
        materialized='table'
    )
}}

SELECT
    invoice_date_day,
    COUNT(DISTINCT invoice_no)              AS invoice_count,
    COUNT(*)                               AS line_items,
    COUNT(DISTINCT customer_id)            AS unique_customers,
    SUM(quantity)                          AS total_units,
    ROUND(SUM(line_total_gbp), 2)          AS gross_revenue_gbp,
    ROUND(AVG(line_total_gbp), 2)          AS avg_line_value_gbp
FROM {{ ref('stg_orders') }}
GROUP BY invoice_date_day
ORDER BY invoice_date_day
