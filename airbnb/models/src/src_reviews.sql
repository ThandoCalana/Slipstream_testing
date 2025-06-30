WITH raw_reviews AS (

    SELECT *
    FROM {{ source('airbnb', 'reviews')}}
)

SELECT 
    listing_id,
    TO_TIMESTAMP_NTZ(date) AS review_date,
    reviewer_name,
    comments AS review_text,
    sentiment AS review_sentiment,
    MD5(CONCAT(CAST(listing_id AS STRING), '-', CAST(TO_DATE(review_date) AS STRING))) AS review_sk,
FROM
    raw_reviews