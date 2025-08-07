{% macro control_tester(test_name_to_run='column_presence_and_types') %}

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
      src.EFFECTIVE_DATE_COL_NAME AS EFFECTIVE_DATE,
      src.EXPIRATION_DATE_COL_NAME   AS EXPIRATION_DATE,
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
    {% set src_model          = row['SRC_DB'] ~ '.' ~ row['SRC_SCHEMA'] ~ '.' ~ row['SRC_TABLE'] %}
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
    {% set trg_model          = row['TRG_DB'] ~ '.' ~ row['TRG_SCHEMA'] ~ '.' ~ row['TRG_TABLE'] %}
    {% set trg_effective_date = row['TRG_EFFECTIVE_DATE'] if row['TRG_EFFECTIVE_DATE'] else NULL %}
    {% set trg_expiration_date = row['TRG_EXPIRATION_DATE'] if row['TRG_EXPIRATION_DATE'] else NULL %}
    {% set trg_layer              = row['TRG_LAYER_TYPE'] if row['TRG_LAYER_TYPE'] else NULL %}


    {% do log("Running test: " ~ test_name ~ " on " ~ src_model ~ " (Config ID: " ~ test_config_id ~ ")", info=True) %}

  


    {% if test_name == "column_presence_and_types" %}
      {% do column_presence_and_types(src_model, src_schema, test_id, test_config_id, trg_model, trg_schema) %}
  
    {% endif %}


  {% endfor %}

{% endmacro %}

