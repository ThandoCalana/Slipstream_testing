{% snapshot dim_customer_scd2 %}

{{
    config(
      target_schema='DEV', 
      unique_key='customer_id', 
      strategy='timestamp',           
      updated_at='load_timestamp',      
      invalidate_hard_deletes=True,
      post_hook="""
        ALTER TABLE {{ this }} ADD COLUMN IF NOT EXISTS is_active BOOLEAN;
        UPDATE {{ this }}
        SET is_active = CASE WHEN expiration_date IS NULL THEN TRUE ELSE FALSE END;
      """      
    )
}}

WITH staged AS (
  SELECT
    customer_id,
    name,
    region,
    dob,
    CAST(load_timestamp AS TIMESTAMPNTZ) AS load_timestamp,
    hash_column,
    {{ dbt_utils.generate_surrogate_key(['customer_id', 'hash_column']) }} AS customer_sk
  FROM {{ ref('src_customers') }}
)

SELECT *
FROM staged

{% endsnapshot %}
