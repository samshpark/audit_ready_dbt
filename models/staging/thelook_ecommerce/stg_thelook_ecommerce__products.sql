WITH source AS (
    SELECT * FROM {{ source('thelook_ecommerce', 'raw_products') }}
),
renamed AS (
    SELECT
        CAST(id AS STRING)                     AS product_id,
        CAST(distribution_center_id AS STRING) AS distribution_center_id,
        LOWER(category)                        AS category,
        name,
        brand,
        department,
        sku,
        CAST(cost AS DOUBLE)                   AS cost,
        CAST(retail_price AS DOUBLE)           AS retail_price
    FROM
        source
)
SELECT * FROM renamed