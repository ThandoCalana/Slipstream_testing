{% macro run_tests_controller() %}

{% set configs_query %}
    SELECT
      config.TEST_CONFIG_ID,
      config.TEST_ID,
      metadata.TEST_NAME,
      metadata.TEST_TYPE,
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
      trg.EXPIRATION_DATE_COL_NAME   AS TRG_EXPIRATION_DATE,     
    FROM AIRBNB.TESTING.TEST_CONFIG config
    JOIN AIRBNB.TESTING.TEST_METADATA metadata ON config.TEST_ID = metadata.TEST_ID
    JOIN AIRBNB.TESTING.ENTITY_METADATA src ON config.SRC_ENTITY_ID = src.ENTITY_ID
    LEFT JOIN AIRBNB.TESTING.ENTITY_METADATA trg ON config.TRG_ENTITY_ID = trg.ENTITY_ID
    WHERE config.is_active = TRUE
      AND metadata.TEST_NAME = '{{ test_name_to_run }}'
  {% endset %}

  {% set configs = run_query(configs_query) %}

  {% for row in configs.rows %}
    {% set test_config_id     = row['TEST_CONFIG_ID'] %}
    {% set test_id            = row['TEST_ID'] %}
    {% set test_name          = row['TEST_NAME'] %}
    {% set test_type          = row['TEST_TYPE'] %}
    {% set src_model          = row['SRC_DB'] ~ '.' ~ row['SRC_SCHEMA'] ~ '.' ~ row['SRC_TABLE'] %}
    {% set src_table          = row['SRC_TABLE'] %}
    {% set src_schema         = row['SRC_SCHEMA'] %}
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
    {% set trg_model          = (row['TRG_DB'] ~ '.' ~ row['TRG_SCHEMA'] ~ '.' ~ row['TRG_TABLE']) if row['TRG_TABLE'] else NULL %}
    {% set trg_table        = row['TRG_TABLE'] if row['TRG_TABLE'] else NULL %}
    {% set trg_schema         = row['TRG_SCHEMA'] if row['TRG_SCHEMA'] else NULL %}
    {% set trg_effective_date = row['TRG_EFFECTIVE_DATE'] if row['TRG_EFFECTIVE_DATE'] else NULL %}
    {% set trg_expiration_date = row['TRG_EXPIRATION_DATE'] if row['TRG_EXPIRATION_DATE'] else NULL %}
    {% set trg_layer              = row['TRG_LAYER_TYPE'] if row['TRG_LAYER_TYPE'] else NULL %}

    {% do log("Running test: " ~ test_name ~ " on " ~ src_model ~ " (Config ID: " ~ test_config_id ~ ")", info=True) %}

    {% set src_filtered = get_filtered_model(src_model, src_load_timestamp)[0] %}
    {% set trg_filtered = get_filtered_model(trg_model, trg_load_timestamp)[0] if trg_model else NULL %}

    {% if test_type == "1-1" %}
      {% do capture_and_update_latest_ts(src_filtered, src_model, src_load_timestamp) %}

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

      {% do capture_and_update_latest_ts(src_filtered, src_model, src_load_timestamp) %}

      {% if test_name == "scd_dummy_records_exist" %}
        {% do scd_dummy_records_exist(src_model, src_pkey, test_id, test_config_id, src_load_timestamp) %}

      {% elif test_name == 'scd_columns_check' %}
        {% do scd_columns_check(src_model, test_id, test_config_id, src_schema, src_load_timestamp) %}

      {% elif test_name == 'scd_unique_active_nkey' %}
        {% do scd_unique_active_nkey(src_model, test_id, test_config_id, src_nkey, src_load_timestamp) %}

      {% elif test_name == 'scd_unique_surrogate_key' %}
        {% do scd_unique_surrogate_key(src_model, test_id, test_config_id, src_skey, src_load_timestamp) %}

      {% elif test_name == 'scd_inactive_historical_records' %}
        {% do scd_inactive_historical_records(src_model, test_id, test_config_id, src_load_timestamp) %}

      {% elif test_name == "scd_not_null_surrogate_key" %}
        {% do scd_not_null_surrogate_key(src_model, test_id, test_config_id, src_skey, src_load_timestamp) %}

      {% elif test_name == 'scd_effective_before_expiry' %}
        {% do scd_effective_before_expiry(src_model, test_id, test_config_id, src_load_timestamp) %}

      {% elif test_name == 'scd_initial_active_state' %}
        {% do scd_initial_active_state(src_model, test_id, test_config_id, src_load_timestamp) %}

      {% elif test_name == "scd_initial_effective_date_set" %}
        {% do scd_initial_effective_date_set(src_model, test_id, test_config_id, src_load_timestamp) %}

      {% elif test_name == "scd_initial_expiry_is_null" %}
        {% do scd_active_null_expiry(src_model, test_id, test_config_id, src_load_timestamp) %}

      {% elif test_name == "scd_active_null_expiry" %}
        {% do scd_active_null_expiry(src_model, test_id, test_config_id, src_load_timestamp) %}

      {% elif test_name == 'scd_no_scd2_valid_from_gaps' %}
        {% do scd_no_scd2_valid_from_gaps(src_model, test_id, test_config_id, src_skey, src_load_timestamp) %}

      {% elif test_name == "scd_sequential_versions" %}
        {% do scd_sequential_versions(src_model, test_id, test_config_id, src_skey, src_load_timestamp) %}

      {% elif test_name == "scd_not_null_active_natural_key" %}
        {% do scd_not_null_active_natural_key(src_model, test_id, test_config_id, src_nkey, src_load_timestamp) %}
      
      {% elif test_name == "scd_sequential_versions" %}
        {% do scd_sequential_versions(src_model, test_id, test_config_id, src_skey, src_load_timestamp) %}
      {% endif %}

    {# {% elif test_type == "Fact"%} #}

    {% else %}
      {% do invoke_macro(test_name, src_model, test_id, test_config_id, src_load_timestamp, trg_load_timestamp) %}
    {% endif %}
  {% endfor %}

{% endmacro %}

{% macro invoke_macro(macro_name, model_name, test_id, test_config_id) %}
  {% if macro_name in context %}
    {% do context[macro_name](model_name, test_id, test_config_id) %}
  {% else %}
    {% do log("Macro '" ~ macro_name ~ "' not found — skipping", info=True) %}
  {% endif %}
{% endmacro %}
