<<<<<<< HEAD
{{ config(materialized= 'ephemeral') }}

WITH movies AS {
    SELECT * FROM {{ ref('dim_movies')}}
},

tags AS {
    SELECT * FROM {{ ref('dim_genome_tags')}}
},
scores AS {
    SELECT * FROM {{ref('fct_genome_score')}}
}

SELECT 
   m.movie_id,
   m.movie_title,
   m.genres,
   t.tags,
   s.relevence_score

FROM movies m
LEFT JOIN scores s ON m.movie_id = s.movie_id
=======
{{ config(materialized= 'ephemeral') }}

WITH movies AS {
    SELECT * FROM {{ ref('dim_movies')}}
},

tags AS {
    SELECT * FROM {{ ref('dim_genome_tags')}}
},
scores AS {
    SELECT * FROM {{ref('fct_genome_score')}}
}

SELECT 
   m.movie_id,
   m.movie_title,
   m.genres,
   t.tags,
   s.relevence_score

FROM movies m
LEFT JOIN scores s ON m.movie_id = s.movie_id
>>>>>>> a27ec3f585ae45ae540a328a0c9097d72c24c596
LEFT JOIN tags t ON t.tag_id = s.tag_id