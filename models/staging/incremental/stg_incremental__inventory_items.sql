WITH source AS (
    SELECT * FROM {{ source('incremental', 'incr_inventory_items') }}
),
renamed AS (
    SELECT
        CAST(id AS STRING)                   AS inventory_item_id,
        CAST(product_id AS STRING)           AS product_id,
        product_distribution_center_id,
        LOWER(product_category)              AS product_category,
        product_name,
        product_brand,
        LOWER(product_department)            AS product_department,
        product_sku,
        CAST(cost AS DOUBLE)                 AS cost,
        CAST(product_retail_price AS DOUBLE) AS product_retail_price,
        CAST(created_at AS TIMESTAMP)        AS created_at,
        CAST(sold_at AS TIMESTAMP)           AS sold_at
    FROM source
)
SELECT * FROM renamed
