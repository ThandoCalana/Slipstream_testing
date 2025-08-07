{% macro run_tests_controller() %}

  {# Step 1: Get max watermark from data lake target table #}
  {% set max_timestamp_query %}
    select max(EFFECTIVE_DATE) as max_ts
    from AIRBNB.DEV.DIM_CUSTOMER_SCD2
  {% endset %}
  {% set results = run_query(max_timestamp_query) %}
  {% set max_ts = '1900-01-01 00:00:00' %}
  {% if results and results.columns[0].values() | length > 0 and results.columns[0].values()[0] is not none %}
    {% set max_ts = results.columns[0].values()[0] %}
  {% endif %}



  {% do log("Using high watermark timestamp: " ~ max_ts, info=True) %}

  {# Step 2: Pass max_ts to filter source data in tests #}
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
      src.EFFECTIVE_DATE AS SRC_EFFECTIVE_DATE,
      src.EXPIRATION_DATE   AS SRC_EXPIRATION_DATE,
      src.LAYER_TYPE,
      config.TRG_ENTITY_ID,
      trg.TABLE_NAME   AS TRG_TABLE,
      trg.TABLE_SCHEMA AS TRG_SCHEMA,
      trg.DB_NAME AS TRG_DB,
      trg.NKEY_COL_NAME AS TRG_NKEY_COL,
      trg.PKEY_COL_NAME AS TRG_PKEY_COL,
      trg.SKEY_COL_NAME AS TRG_SKEY_COL,
      trg.HASH_COL_NAME AS TRG_HASH_COL,
      trg.TIMESTAMP_COL_NAME AS TRG_LOAD_TIMESTAMP,
      trg.EFFECTIVE_DATE AS TRG_EFFECTIVE_DATE,
      trg.EXPIRATION_DATE   AS TRG_EXPIRATION_DATE       
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
    {% set src_model          = row['SRC_TABLE'] %}
    {% set src_schema         = row['SRC_SCHEMA'] %}
    {% set src_db             = row['SRC_DB'] %}
    {% set src_load_timestamp = row['SRC_LOAD_TIMESTAMP'] %}
    {% set trg_load_timestamp = row['TRG_LOAD_TIMESTAMP'] if row['TRG_LOAD_TIMESTAMP'] else NULL %}
    {% set src_nkey           = row['SRC_NKEY_COL'] %}
    {% set src_pkey           = row['SRC_PKEY_COL'] %}
    {% set src_skey           = row['SRC_SKEY_COL'] if row['SRC_SKEY_COL'] else NULL %}
    {% set src_hash_col       = row['SRC_HASH_COL'] if row['SRC_HASH_COL'] else NULL %}
    {% set src_effective_date = row['SRC_EFFECTIVE_DATE'] %}
    {% set src_expiration_date    = row['SRC_EXPIRATION_DATE'] %}
    {% set layer              = row['LAYER_TYPE'] %}
    {% set trg_nkey           = row['TRG_NKEY_COL'] %}
    {% set trg_pkey           = row['TRG_PKEY_COL'] %}
    {% set trg_skey           = row['TRG_SKEY_COL'] if row['TRG_SKEY_COL'] else NULL %}
    {% set trg_hash_col       = row['TRG_HASH_COL'] if row['TRG_HASH_COL'] else NULL %}
    {% set trg_model          = row['TRG_TABLE'] if row['TRG_TABLE'] else NULL%}
    {% set trg_schema         = row['TRG_SCHEMA'] if row['TRG_SCHEMA'] else NULL%}
    {% set trg_db             = row['TRG_DB'] if row['TRG_DB'] else NULL%}
    {% set trg_effective_date = row['TRG_EFFECTIVE_DATE'] if row['TRG_EFFECTIVE_DATE'] else NULL %}
    {% set trg_expiration_date = row['TRG_EXPIRATION_DATE'] if row['TRG_EXPIRATION_DATE'] else NULL %}

    {# Step 3: Build filtered source model name #}
    {% set filtered_src_model = src_model ~ '_filtered' %}

    {# Step 4: Create filtered source view based on high watermark #}
    {% set create_filtered_src %}
      create or replace view {{ filtered_src_model }} as
      select *
      from {{ src_schema }}.{{ src_model }}
      WHERE {{ effective_date }} > {{ max_ts }}
    {% endset %}
    {% do run_query(create_filtered_src) %}
    
    {# Step 5: Create filtered target view based on high watermark #}
    {% set create_filtered_trg %}
      create or replace view {{ filtered_trg_model }} as
      select *
      from {{ trg_schema }}.{{ trg_model }}
      WHERE {{ effective_date }} > {{ max_ts }}
    {% endset %}
    {% do run_query(create_filtered_trg) %}

    {% do log("Running " ~ test_name ~ " on filtered source " ~ filtered_src_model ~ " (Config ID: " ~ test_config_id ~ ")", info=True) %}


    {% if test_name == "row_count_match" %}
      {% do row_count_match(filtered_src_model, src_schema, src_db, test_id, test_config_id, filtered_trg_model, trg_schema, trg_db) %}

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
        {% do not_null_key(filtered_src_model, src_schema, src_db, test_id, test_config_id, keys) %}
      {% endif %}

    {% elif test_name == "unique_key" %}
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
        {% do unique_key(filtered_src_model, src_schema, src_db, test_id, test_config_id, keys) %}
      {% endif %}

    {% elif test_name == "hash_match" %}
      {% do hash_match(filtered_src_model, src_schema, src_db, src_pkey, src_hash_col, src_load_timestamp, test_id, test_config_id, filtered_trg_model, trg_schema, trg_db, trg_pkey, trg_hash_col, trg_load_timestamp) %}

    {% elif test_name == "column_presence_and_types" %}
      {% do column_presence_and_type(filtered_src_model, src_schema, test_id, test_config_id, filtered_trg_model, trg_schema) %}

    {% elif test_name == "scd_dummy_records_exist" %}
      {% do scd_dummy_records_exist(src_model, src_schema, src_db, src_pkey, test_id, test_config_id) %}

    {% elif test_name == 'scd_columns_check' %}
      {% do scd_columns_check(filtered_src_model, test_id, test_config_id, src_schema) %}

    {% elif test_name == 'scd_unique_active_nkey' %}
      {% do scd_unique_active_nkey(filtered_src_model, test_id, test_config_id, src_nkey) %}

    {% elif test_name == 'scd_unique_surrogate_key' %}
      {% do scd_unique_surrogate_key(filtered_src_model, test_id, test_config_id, src_skey) %}

    {% elif test_name == 'scd_inactive_historical_records' %}
      {% do scd_inactive_historical_records(filtered_src_model, test_id, test_config_id) %}

    {% elif test_name == "scd_not_null_surrogate_key" %}
      {% do scd_not_null_surrogate_key(filtered_src_model, test_id, test_config_id, src_skey) %}

    {% elif test_name == 'scd_effective_before_expiry' %}
      {% do scd_effective_before_expiry(filtered_src_model, test_id, test_config_id) %}

    {% elif test_name == 'scd_initial_active_state' %}
      {% do scd_initial_active_state(filtered_src_model, test_id, test_config_id) %}

    {% elif test_name == "scd_initial_effective_date_set" %}
      {% do scd_initial_effective_date_set(filtered_src_model, test_id, test_config_id) %}

    {% elif test_name == "scd_initial_expiry_is_null" %}
      {% do scd_active_null_expiry(filtered_src_model, test_id, test_config_id) %}

    {% elif test_name == "scd_active_null_expiry" %}
      {% do scd_active_null_expiry(filtered_src_model, test_id, test_config_id) %}

    {% elif test_name == 'scd_no_scd2_valid_from_gaps' %}
      {% do scd_no_scd2_valid_from_gaps(filtered_src_model, test_id, test_config_id, src_skey) %}

    {% elif test_name == "scd_sequential_versions" %}
      {% do scd_sequential_versions(filtered_src_model, test_id, test_config_id, src_skey) %}

    {% elif test_name == "scd_not_null_active_natural_key" %}
      {% do scd_not_null_active_natural_key(filtered_src_model, test_id, test_config_id, src_nkey) %}
    
    {% elif test_name == "scd_sequential_versions" %}
      {% do scd_sequential_versions(filtered_src_model, test_id, test_config_id, src_skey) %}

    {% else %}
      {% do invoke_macro(test_name, filtered_src_model, test_id, test_config_id) %}
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
