WITH source AS (
    SELECT * FROM {{ source('thelook_ecommerce', 'raw_inventory_items') }}
),
renamed AS (
    SELECT
        CAST(id AS STRING) AS inventory_item_id,
        CAST(product_id AS STRING) AS product_id,
        CAST(created_at AS TIMESTAMP) AS created_at,
        CAST(sold_at AS TIMESTAMP) AS sold_at,
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
SELECT * FROM renamed