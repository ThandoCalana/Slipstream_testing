{% snapshot dim_tags_scd %}

{{
    config (
        target_database = 'dbt_prac',
        unique_key=['userid', 'movieid'],
        strategy='timestamp',
        updated_at='timestamp'
    )
}}


select * from dbt_prac.dbt_source.dim_tags_scd

{% endsnapshot %}