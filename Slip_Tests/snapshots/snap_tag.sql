<<<<<<< HEAD
{% snapshot snap_tags %}

{{
    config(
        target_schema='snapshots',
        unique_key=['user_id', 'movie_id', 'tag'],
        strategy='timestamp',
        updated_at='tag_timestamp',
        invalidate_hard_deletes=True
    )
}}



SELECT
    {{ dbt_utils.generate_surrogate_key(['user_id', 'movie_id']) }} AS id,
    user_id,
    movie_id,
    tag,
    tag_timestamp
FROM {{ ref('src_tags') }}
-- LIMIT 100

{% endsnapshot %}
=======
{% snapshot snap_tags %}

{{
    config(
        target_schema='snapshots',
        unique_key=['user_id', 'movie_id', 'tag'],
        strategy='timestamp',
        updated_at='tag_timestamp',
        invalidate_hard_deletes=True
    )
}}



SELECT
    {{ dbt_utils.generate_surrogate_key(['user_id', 'movie_id']) }} AS id,
    user_id,
    movie_id,
    tag,
    tag_timestamp
FROM {{ ref('src_tags') }}
-- LIMIT 100

{% endsnapshot %}
>>>>>>> a27ec3f585ae45ae540a328a0c9097d72c24c596
