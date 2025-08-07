SELECT
  order_id,
  customer_id,
  product_id,
  quantity::NUMBER AS quantity,
  amount::NUMBER AS amount,
  order_date::DATE AS order_date,
  CAST(load_timestamp AS TIMESTAMPNTZ) AS load_timestamp,
  hash_column
FROM {{ source('airbnb', 'raw_orders') }}
