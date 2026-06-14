WITH source AS (
    SELECT * FROM {{source('thelook_ecommerce', 'raw_order_items')}}
),
reformat AS (
    SELECT
        CAST(id AS STRING) AS order_item_id, -- make key value integer -> string
        CAST(order_id AS STRING) AS order_id,
        CAST(user_id AS STRING) AS user_id,
        CAST(product_id AS STRING) AS product_id,
        CAST(inventory_item_id AS STRING) AS inventory_item_id,
        LOWER(status) AS order_item_status,
        CAST(created_at AS TIMESTAMP) AS created_at, -- UTC timestamp
        CAST(shipped_at AS TIMESTAMP) AS shipped_at,
        CAST(delivered_at AS TIMESTAMP) AS delivered_at,
        CAST(returned_at AS TIMESTAMP) AS returned_at,
        CAST(sale_price AS DOUBLE) AS sale_price
    FROM
        source
)
SELECT * FROM reformat