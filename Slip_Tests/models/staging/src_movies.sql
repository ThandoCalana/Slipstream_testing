<<<<<<< HEAD
WITH raw_movies AS (
   SELECT * FROM MOVIELENS.RAW.RAW_MOVIES
)
SELECT
    movieId As movie_id,
    title,
    genres
=======
WITH raw_movies AS (
   SELECT * FROM MOVIELENS.RAW.RAW_MOVIES
)
SELECT
    movieId As movie_id,
    title,
    genres
>>>>>>> a27ec3f585ae45ae540a328a0c9097d72c24c596
FROM raw_movies