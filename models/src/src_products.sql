SELECT
  product_id,
  item,
  category,
  price::NUMBER AS price,
  load_timestamp,
  hash_column
FROM {{ source('airbnb', 'raw_products') }}
