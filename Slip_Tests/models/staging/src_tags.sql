<<<<<<< HEAD
{{ config(materialized = 'table') }}

WITH raw_tags AS (
    SELECT * FROM MOVIELENS.RAW.RAW_TAGS
)
SELECT
    userId AS user_id,
    movieId AS movie_id,
    tag,
    TO_TIMESTAMP_NTZ(timestamp) AS tag_timestamp
=======
{{ config(materialized = 'table') }}

WITH raw_tags AS (
    SELECT * FROM MOVIELENS.RAW.RAW_TAGS
)
SELECT
    userId AS user_id,
    movieId AS movie_id,
    tag,
    TO_TIMESTAMP_NTZ(timestamp) AS tag_timestamp
>>>>>>> a27ec3f585ae45ae540a328a0c9097d72c24c596
FROM raw_tags