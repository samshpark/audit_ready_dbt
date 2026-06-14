WITH source AS (
    SELECT * FROM {{ source('thelook_ecommerce', 'raw_users') }}
),
renamed AS (
    SELECT
        CAST(id AS STRING) AS user_id,
        LOWER(first_name) AS first_name,
        LOWER(last_name) AS last_name,
        LOWER(email) AS email,
        age,
        LOWER(gender) AS gender,
        LOWER(state) AS state,
        LOWER(street_address) AS street_address,
        postal_code,
        LOWER(city) AS city,
        LOWER(country) AS country,
        latitude,
        longitude,
        LOWER(traffic_source) AS traffic_source,
        CAST(created_at AS TIMESTAMP) AS created_at,
        user_geom
    FROM
        source
)
SELECT * FROM renamed