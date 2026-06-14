WITH source AS (
    SELECT * FROM {{source('thelook_ecommerce', 'raw_products')}}
),
reformat AS (
    SELECT
        CAST(id AS STRING) AS product_id, -- make key value integer -> string
        CAST(cost AS DOUBLE) AS cost, -- BigQuery FLOAT64 is mapped to DOUBLE in DuckDB for precision
        LOWER(category) AS category,
        name,
        brand,
        CAST(retail_price AS DOUBLE) AS retail_price, -- BigQuery FLOAT64 is mapped to DOUBLE in DuckDB for precision
        department,
        sku,
        CAST(distribution_center_id AS STRING) AS distribution_center_id
    FROM
        source
)
SELECT * FROM reformat