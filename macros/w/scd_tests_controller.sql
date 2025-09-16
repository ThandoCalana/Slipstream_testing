{% macro run_tests_controller() %}

  {% set unique_tables_query %}
    SELECT DISTINCT
      src.ENTITY_ID,
      src.DB_NAME || '.' || src.TABLE_SCHEMA || '.' || src.TABLE_NAME AS SRC_MODEL,
      src.TIMESTAMP_COL_NAME AS SRC_LOAD_TIMESTAMP,
      trg.DB_NAME || '.' || trg.TABLE_SCHEMA || '.' || trg.TABLE_NAME AS TRG_MODEL,
    FROM AIRBNB.TESTING.TEST_CONFIG config
    JOIN AIRBNB.TESTING.TEST_METADATA metadata ON config.TEST_ID = metadata.TEST_ID
    JOIN AIRBNB.TESTING.ENTITY_METADATA src ON config.SRC_ENTITY_ID = src.ENTITY_ID
    LEFT JOIN AIRBNB.TESTING.ENTITY_METADATA trg ON config.TRG_ENTITY_ID = trg.ENTITY_ID
    WHERE config.is_active = TRUE
      AND metadata.TEST_NAME = '{{ test_name_to_run }}'
  {% endset %}

  {% set unique_tables = run_query(unique_tables_query) %}
  
  {{ log("Updating HWM for " ~ unique_tables.rows | length ~ " unique tables before running tests", info=True) }}

  {% for table_row in unique_tables.rows %}
    {% set src_model = table_row['SRC_MODEL'] %}
    {% set trg_model = table_row['TRG_MODEL'] %}
    {% set src_load_timestamp = table_row['SRC_LOAD_TIMESTAMP'] %}
    {% set entity_id = table_row['ENTITY_ID'] %}
    
    {% if trg_model %}

      {% do capture_and_update_latest_ts(src_model, src_load_timestamp) %}

    {% endif %}

    {{ log("Updated HWM for: " ~ src_model, info=True) }}

  {% endfor %}

  {% set configs_query %}
    SELECT
      config.TEST_CONFIG_ID,
      config.TEST_ID,
      metadata.TEST_NAME,
      src.TABLE_NAME   AS SRC_TABLE,
      src.TABLE_SCHEMA AS SRC_SCHEMA,
      src.DB_NAME AS SRC_DB,
      src.NKEY_COL_NAME AS SRC_NKEY_COL,
      src.PKEY_COL_NAME AS SRC_PKEY_COL,
      src.SKEY_COL_NAME AS SRC_SKEY_COL,
      src.HASH_COL_NAME AS SRC_HASH_COL,
      src.TIMESTAMP_COL_NAME AS SRC_LOAD_TIMESTAMP,
      src.EFFECTIVE_DATE_COL_NAME AS SRC_EFFECTIVE_DATE,
      src.EXPIRATION_DATE_COL_NAME   AS SRC_EXPIRATION_DATE,
      src.LAYER_TYPE AS SRC_LAYER_TYPE,
      src.IS_ACTIVE_COL_NAME AS SRC_IS_ACTIVE,
      config.TRG_ENTITY_ID,
      trg.TABLE_NAME   AS TRG_TABLE,
      trg.TABLE_SCHEMA AS TRG_SCHEMA,
      trg.DB_NAME AS TRG_DB,
      trg.LAYER_TYPE AS TRG_LAYER_TYPE,
      trg.NKEY_COL_NAME AS TRG_NKEY_COL,
      trg.PKEY_COL_NAME AS TRG_PKEY_COL,
      trg.SKEY_COL_NAME AS TRG_SKEY_COL,
      trg.HASH_COL_NAME AS TRG_HASH_COL,
      trg.TIMESTAMP_COL_NAME AS TRG_LOAD_TIMESTAMP,
      trg.EFFECTIVE_DATE_COL_NAME AS TRG_EFFECTIVE_DATE,
      trg.EXPIRATION_DATE_COL_NAME   AS TRG_EXPIRATION_DATE     
    FROM AIRBNB.TESTING.TEST_CONFIG config
    JOIN AIRBNB.TESTING.TEST_METADATA metadata ON config.TEST_ID = metadata.TEST_ID
    JOIN AIRBNB.TESTING.ENTITY_METADATA src ON config.SRC_ENTITY_ID = src.ENTITY_ID
    LEFT JOIN AIRBNB.TESTING.ENTITY_METADATA trg ON config.TRG_ENTITY_ID = trg.ENTITY_ID
    WHERE config.is_active = TRUE
  {% endset %}

  {% set configs = run_query(configs_query) %}

  {% for row in configs.rows %}
    {% set test_config_id     = row['TEST_CONFIG_ID'] %}
    {% set test_id            = row['TEST_ID'] %}
    {% set test_name          = row['TEST_NAME'] %}
    {% set src_model          = row['SRC_DB'] ~ '.' ~ row['SRC_SCHEMA'] ~ '.' ~ row['SRC_TABLE'] %}
    {% set src_schema         = row['SRC_SCHEMA'] %}
    {% set src_table          = row['SRC_TABLE'] %}
    {% set src_is_active      = row['SRC_IS_ACTIVE'] %}
    {% set src_load_timestamp = row['SRC_LOAD_TIMESTAMP'] %}
    {% set trg_load_timestamp = row['TRG_LOAD_TIMESTAMP'] if row['TRG_LOAD_TIMESTAMP'] else NULL %}
    {% set src_nkey           = row['SRC_NKEY_COL'] %}
    {% set src_pkey           = row['SRC_PKEY_COL'] %}
    {% set src_skey           = row['SRC_SKEY_COL'] if row['SRC_SKEY_COL'] else NULL %}
    {% set src_hash_col       = row['SRC_HASH_COL'] if row['SRC_HASH_COL'] else NULL %}
    {% set src_effective_date     = row['SRC_EFFECTIVE_DATE'] %}
    {% set src_expiration_date    = row['SRC_EXPIRATION_DATE'] %}
    {% set src_layer              = row['SRC_LAYER_TYPE'] %}
    {% set trg_nkey           = row['TRG_NKEY_COL'] %}
    {% set trg_pkey           = row['TRG_PKEY_COL'] %}
    {% set trg_skey           = row['TRG_SKEY_COL'] if row['TRG_SKEY_COL'] else NULL %}
    {% set trg_hash_col       = row['TRG_HASH_COL'] if row['TRG_HASH_COL'] else NULL %}
    {% set trg_model          = row['TRG_DB'] ~ '.' ~ row['TRG_SCHEMA'] ~ '.' ~ row['TRG_TABLE'] if row['TRG_TABLE'] else NULL %}
    {% set trg_table        = row['TRG_TABLE'] if row['TRG_TABLE'] else NULL %}
    {% set trg_effective_date = row['TRG_EFFECTIVE_DATE'] if row['TRG_EFFECTIVE_DATE'] else NULL %}
    {% set trg_expiration_date = row['TRG_EXPIRATION_DATE'] if row['TRG_EXPIRATION_DATE'] else NULL %}
    {% set trg_layer              = row['TRG_LAYER_TYPE'] if row['TRG_LAYER_TYPE'] else NULL %}

    {{ log("Running test: " ~ test_name ~ " on " ~ src_model ~ " (Config ID: " ~ test_config_id ~ ")", info=True) }}

    {% if test_type == "1-1" %}
      
      {% if test_name == "row_count_match" %}
        {% do row_count_match(src_model, test_id, test_config_id, trg_model, src_load_timestamp, trg_load_timestamp) %}
      
      {% elif test_name == "not_null_key" %}
        {% set keys = [] %}
        {% if src_nkey is not none and src_nkey | trim != '' %}
          {% do keys.append(src_nkey) %}
        {% endif %}
        {% if src_pkey is not none and src_pkey | trim != '' %}
          {% do keys.append(src_pkey) %}
        {% endif %}
        {% if src_skey is not none and src_skey | trim != '' %}
          {% do keys.append(src_skey) %}
        {% endif %}

        {% if keys | length > 0 %}
          {% do not_null_key(src_model, test_id, test_config_id, src_load_timestamp, keys) %}
        {% endif %}

      {% elif test_name == "unique_records" %}
        {% do unique_records(src_model, test_id, test_config_id, src_hash_col, src_load_timestamp) %}

      {% elif test_name == "hash_match" %}
        {% do hash_match(src_model, src_pkey, src_hash_col, test_id, test_config_id, 
          trg_model, trg_pkey, trg_hash_col, src_load_timestamp, trg_load_timestamp) %}

      {% elif test_name == "column_presence_and_types" %}
        {% do column_presence_and_type(src_model, src_schema, test_id, test_config_id, trg_model, trg_schema) %}

      {% endif %}
      
    {% elif test_type == "dim_scd2"%}

      {% if test_name == "scd_dummy_records_exist" %}
        {% do scd_dummy_records_exist(src_model, src_pkey, test_id, test_config_id, src_load_timestamp) %}

      {% elif test_name == 'scd_columns_check' %}
        {% do scd_columns_check(src_model, test_id, test_config_id, src_schema, src_load_timestamp) %}

      {% elif test_name == 'scd_unique_active_nkey' %}
        {% do scd_unique_active_nkey(src_model, test_id, test_config_id, src_nkey, src_load_timestamp, src_is_active) %}

      {% elif test_name == 'scd_unique_surrogate_key' %}
        {% do scd_unique_surrogate_key(src_model, test_id, test_config_id, src_skey, src_load_timestamp) %}

      {% elif test_name == 'scd_inactive_historical_records' %}
        {% do scd_inactive_historical_records(src_model, test_id, test_config_id, src_load_timestamp, src_is_active, src_expiration_date) %}

      {% elif test_name == "scd_not_null_surrogate_key" %}
        {% do scd_not_null_surrogate_key(src_model, test_id, test_config_id, src_skey, src_load_timestamp) %}

      {% elif test_name == 'scd_effective_before_expiry' %}
        {% do scd_effective_before_expiry(src_model, src_nkey, test_id, test_config_id, src_load_timestamp, src_effective_date, src_expiration_date) %}

      {% elif test_name == "scd_active_null_expiry" %}
        {% do scd_active_null_expiry(src_model, test_id, test_config_id, src_load_timestamp, src_is_active, src_expiration_date) %}

      {% elif test_name == 'scd_no_scd2_effective_date_gaps' %}
        {% do scd_no_scd2_effective_date_gaps(src_model, test_id, test_config_id, src_skey, src_load_timestamp, src_effective_date, src_expiration_date) %}

      {% elif test_name == "scd_sequential_versions" %}
        {% do scd_sequential_versions(src_model, test_id, test_config_id, src_skey, src_load_timestamp) %}

      {% elif test_name == "scd_not_null_active_natural_key" %}
        {% do scd_not_null_active_natural_key(src_model, test_id, test_config_id, src_nkey, src_load_timestamp, src_is_active) %}
      
      {% endif %}

    {% else %}
      {% do invoke_macro(test_name, src_model, test_id, test_config_id, src_load_timestamp, trg_load_timestamp) %}
    {% endif %}
  {% endfor %}

{% endmacro %}