WITH source AS (
    SELECT * FROM {{ source('thelook_ecommerce', 'raw_products') }}
),
renamed AS (
    SELECT
        CAST(id AS STRING) AS product_id,
        CAST(cost AS DOUBLE) AS cost,
        LOWER(category) AS category,
        name,
        brand,
        CAST(retail_price AS DOUBLE) AS retail_price,
        department,
        sku,
        CAST(distribution_center_id AS STRING) AS distribution_center_id
    FROM
        source
)
SELECT * FROM renamed