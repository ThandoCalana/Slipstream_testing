<<<<<<< HEAD
WITH src_tags AS (
    SELECT * FROM {{ ref('src_genome_tags') }}
)

SELECT
    tag_id,
    INITCAP(TRIM(tag)) AS tag_name
FROM src_tags
=======
WITH src_tags AS (
    SELECT * FROM {{ ref('src_genome_tags') }}
)

SELECT
    tag_id,
    INITCAP(TRIM(tag)) AS tag_name
FROM src_tags
>>>>>>> a27ec3f585ae45ae540a328a0c9097d72c24c596
