<<<<<<< HEAD
WITH ratings AS (
    SELECT DISTINCT user_id FROM {{ ref('src_ratings') }}
),

tags AS (
    SELECT DISTINCT user_id FROM {{ ref('src_tags') }}
)

SELECT DISTINCT user_id
FROM (
    SELECT * FROM ratings
    UNION
    SELECT * FROM tags
) 
=======
WITH ratings AS (
    SELECT DISTINCT user_id FROM {{ ref('src_ratings') }}
),

tags AS (
    SELECT DISTINCT user_id FROM {{ ref('src_tags') }}
)

SELECT DISTINCT user_id
FROM (
    SELECT * FROM ratings
    UNION
    SELECT * FROM tags
) 
>>>>>>> a27ec3f585ae45ae540a328a0c9097d72c24c596
