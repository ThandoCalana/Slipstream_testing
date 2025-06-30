SELECT *
FROM (
    {{ data_match(
        source_table = ref('src_reviews'),
        target_table = ref('fct_reviews'),
        surrogate_key = 'review_sk',
        columns_to_compare = ['listing_id', 'review_date', 'review_text', 'review_sentiment']
    ) }}
) AS mismatches
