WITH source AS (
    SELECT * FROM {{source('thelook_ecommerce', 'raw_inventory_items')}}
),
reformat AS (
    SELECT
        CAST(id AS STRING) AS inventory_item_id, -- make key value integer -> string
        CAST(product_id AS STRING) AS product_id,
        strftime(CAST(created_at AS TIMESTAMP), '%Y-%m-%d %H:%M:%S') AS created_at, -- UTC timestamp
        strftime(CAST(sold_at AS TIMESTAMP), '%Y-%m-%d %H:%M:%S') AS sold_at,
        CAST(cost AS DOUBLE) AS cost,
        LOWER(product_category) AS product_category,
        product_name,
        product_brand,
        CAST(product_retail_price AS DOUBLE) AS product_retail_price,
        LOWER(product_department) AS product_department,
        product_sku,
        product_distribution_center_id
    FROM
        source
)
SELECT * FROM reformat