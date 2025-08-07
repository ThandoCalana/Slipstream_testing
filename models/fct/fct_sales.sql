{{ config(
    materialized='table'
) }}

SELECT
  {{ dbt_utils.generate_surrogate_key(['order_id']) }} AS sales_sk,
  o.order_id,
  o.customer_id,
  o.product_id,
  p.product_sk,
  o.quantity,
  o.amount,
  o.order_date,
  o.load_timestamp,
  o.hash_column
FROM {{ ref('src_orders') }} o
LEFT JOIN {{ ref('dim_product') }} p ON o.product_id = p.product_id
