WITH raw_revs AS (
    SELECT * FROM AIRBNB.RAW.REVIEWS
)
SELECT 
    listing_id,
    review_date,
    reviewer_name,
    review_text,
    review_sentiment,
    batch_id
FROM raw_revs