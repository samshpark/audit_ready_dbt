with source as (
    select * from {{ source('thelook_ecommerce', 'raw_users') }}
),

renamed as (
    select
        cast(id as string) as user_id,
        lower(first_name) as first_name,
        lower(last_name) as last_name,
        lower(email) as email,
        lower(gender) as gender,
        lower(state) as state,
        lower(street_address) as street_address,
        postal_code,
        lower(city) as city,
        lower(country) as country,
        lower(traffic_source) as traffic_source,
        age,
        latitude,
        longitude,
        cast(created_at as timestamp) as created_at,
        user_geom
    from
        source
)

select * from renamed
