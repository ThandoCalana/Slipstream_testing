SELECT
  {{ dbt_utils.generate_surrogate_key(['product_id']) }} AS product_sk,
  product_id,
  item,
  category,
  price,
  load_timestamp,
  hash_column
FROM {{ ref('src_products') }}
