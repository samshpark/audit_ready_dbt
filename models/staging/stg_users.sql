WITH source AS (
    SELECT * FROM {{source('thelook_ecommerce', 'raw_users')}}
),
reformat AS (
    SELECT
        CAST(id as string) AS user_id, -- make key value integer -> string
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
        created_at,
        user_geom
    FROM
        source
)
SELECT * FROM reformat