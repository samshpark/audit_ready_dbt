-- Save as result as table
{{ config(materialized = 'table') }}

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

order_items AS (
    SELECT * FROM {{ ref('stg_order_items') }}
),

products AS (
    SELECT * FROM {{ ref('stg_products') }}
)

SELECT
    a.order_item_id,
    b.order_id
FROM order_items a
LEFT JOIN orders b ON a.order_id = b.order_id
LEFT JOIN products c ON a.product_id = c.product_id
