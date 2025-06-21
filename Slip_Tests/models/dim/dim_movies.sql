<<<<<<< HEAD
WITH src_movies AS (
    SELECT * FROM {{ ref('src_movies') }}
)

SELECT
    movie_id,
    INITCAP(TRIM(title)) AS movie_title,
    SPLIT(genres, '|') AS genre_array,
FROM src_movies
=======
WITH src_movies AS (
    SELECT * FROM {{ ref('src_movies') }}
)

SELECT
    movie_id,
    INITCAP(TRIM(title)) AS movie_title,
    SPLIT(genres, '|') AS genre_array,
FROM src_movies
>>>>>>> a27ec3f585ae45ae540a328a0c9097d72c24c596
