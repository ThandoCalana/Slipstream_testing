WITH raw_listings AS (

    SELECT * 
    FROM {{ source('airbnb', 'listings')}}
)

SELECT
    listing_id,
    name AS listing_name,
    listing_url,
    room_type,
    minimum_nights,
    host_id,
    price AS price_str,
    TO_TIMESTAMP_NTZ(created_at) AS created_at,
    TO_TIMESTAMP_NTZ(updated_at) AS updated_at
FROM
    raw_listings