<<<<<<< HEAD
-- For links
WITH raw_links AS (
    SELECT * FROM MOVIELENS.RAW.RAW_LINKS
)

SELECT
    movieId AS movie_id,
    imdbId AS imdb_id,
    tmdbId AS tmdb_id
FROM raw_links
=======
-- For links
WITH raw_links AS (
    SELECT * FROM MOVIELENS.RAW.RAW_LINKS
)

SELECT
    movieId AS movie_id,
    imdbId AS imdb_id,
    tmdbId AS tmdb_id
FROM raw_links
>>>>>>> a27ec3f585ae45ae540a328a0c9097d72c24c596
