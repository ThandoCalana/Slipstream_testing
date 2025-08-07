SELECT
  customer_id,
  name,
  region,
  dob::DATE AS dob,
  CAST(load_timestamp AS TIMESTAMPNTZ) AS load_timestamp,
  hash_column
FROM {{ source('airbnb', 'raw_customers') }}
