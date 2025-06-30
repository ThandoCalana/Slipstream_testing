{{
    config(
        materialized='incremental',
        on_schema_change='fail'
    )
}}

WITH src_reviews AS (
    SELECT * FROM {{ ref('src_reviews') }}
)

SELECT 
    listing_id,
    review_date,
    reviewer_name,
    review_text,
    review_sentiment,
    MD5(CONCAT(CAST(listing_id AS STRING), '-', CAST(TO_DATE(review_date) AS STRING))) AS review_sk
FROM src_reviews
WHERE review_text IS NOT NULL

{% if is_incremental() %}
  AND review_date > (SELECT MAX(review_date) FROM {{ this }})
{% endif %}
