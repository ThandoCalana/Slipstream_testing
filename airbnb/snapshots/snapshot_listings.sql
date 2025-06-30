{% snapshot snapshot_listings %}

{{
   config(
       target_schema='raw',
       unique_key='id',
       strategy='timestamp',
       updated_at='updated_at',
       invalidate_hard_deletes=True
   )
}}

SELECT * FROM {{source('airbnb', 'listings')}}

{% endsnapshot %}