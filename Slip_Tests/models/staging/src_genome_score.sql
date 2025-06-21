<<<<<<< HEAD
-- For genome_scores
WITH raw_genome_scores AS (
    SELECT * FROM MOVIELENS.RAW.RAW_GENOME_SCORES
)

SELECT
    movieId AS movie_id,
    tagId AS tag_id,
    relevance
FROM raw_genome_scores
=======
-- For genome_scores
WITH raw_genome_scores AS (
    SELECT * FROM MOVIELENS.RAW.RAW_GENOME_SCORES
)

SELECT
    movieId AS movie_id,
    tagId AS tag_id,
    relevance
FROM raw_genome_scores
>>>>>>> a27ec3f585ae45ae540a328a0c9097d72c24c596
