<<<<<<< HEAD
WITH raw_genome_tags AS (
    SELECT * FROM MOVIELENS.RAW.RAW_GENOME_TAGS
)

SELECT
    tagId AS tag_id,
    tag
FROM raw_genome_tags
=======
WITH raw_genome_tags AS (
    SELECT * FROM MOVIELENS.RAW.RAW_GENOME_TAGS
)

SELECT
    tagId AS tag_id,
    tag
FROM raw_genome_tags
>>>>>>> a27ec3f585ae45ae540a328a0c9097d72c24c596
