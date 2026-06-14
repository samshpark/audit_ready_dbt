WITH source AS (
    SELECT * FROM {{ source('thelook_ecommerce', 'raw_orders') }}
),
renamed AS (
    SELECT
        CAST(order_id AS STRING) AS order_id,
        CAST(user_id AS STRING) AS user_id,
        LOWER(status) AS order_status,
        LOWER(gender) AS user_gender,
        CAST(created_at AS TIMESTAMP) AS created_at,
        CAST(returned_at AS TIMESTAMP) AS returned_at,
        CAST(shipped_at AS TIMESTAMP) AS shipped_at,
        CAST(delivered_at AS TIMESTAMP) AS delivered_at,
        num_of_item
    FROM
        source
)
SELECT * FROM renamed