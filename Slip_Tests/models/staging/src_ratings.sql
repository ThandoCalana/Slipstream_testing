<<<<<<< HEAD
{{ config(materialized = 'table') }}

WITH raw_ratings AS (

    SELECT * FROM MOVIELENS.RAW.RAW_RATINGS
)

SELECT
    userId AS user_id,
    movieId AS movie_id,
    rating,
    TO_TIMESTAMP_LTZ(timestamp) AS rating_timestamp
=======
{{ config(materialized = 'table') }}

WITH raw_ratings AS (

    SELECT * FROM MOVIELENS.RAW.RAW_RATINGS
)

SELECT
    userId AS user_id,
    movieId AS movie_id,
    rating,
    TO_TIMESTAMP_LTZ(timestamp) AS rating_timestamp
>>>>>>> a27ec3f585ae45ae540a328a0c9097d72c24c596
FROM raw_ratings