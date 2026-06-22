{{
    config(
        materialized='table'
    )
}}

SELECT
    country,
    COUNT(DISTINCT invoice_no)              AS invoice_count,
    COUNT(DISTINCT customer_id)            AS unique_customers,
    SUM(quantity)                          AS total_units,
    ROUND(SUM(line_total_gbp), 2)          AS gross_revenue_gbp,
    ROUND(
        100.0 * SUM(line_total_gbp)
        / SUM(SUM(line_total_gbp)) OVER (),
        2
    )                                      AS pct_of_total
FROM {{ ref('stg_orders') }}
GROUP BY country
ORDER BY gross_revenue_gbp DESC
