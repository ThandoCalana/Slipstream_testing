<<<<<<< HEAD

WITH src_scores AS (
    SELECT * FROM {{ ref('src_genome_score') }}
)

SELECT
    movie_id,
    tag_id,
    ROUND(relevance, 4) AS relevance_score
FROM src_scores
WHERE relevance > 0
=======

WITH src_scores AS (
    SELECT * FROM {{ ref('src_genome_score') }}
)

SELECT
    movie_id,
    tag_id,
    ROUND(relevance, 4) AS relevance_score
FROM src_scores
WHERE relevance > 0
>>>>>>> a27ec3f585ae45ae540a328a0c9097d72c24c596
