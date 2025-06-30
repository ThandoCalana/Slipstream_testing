WITH raw_hosts AS (

    SELECT * 
    FROM {{ source('airbnb', 'hosts')}}
)

SELECT
    host_id, 
    name AS host_name,
    is_superhost,
    TO_TIMESTAMP_NTZ(created_at) AS created_at,
    TO_TIMESTAMP_NTZ(updated_at) AS updated_at
FROM raw_hosts