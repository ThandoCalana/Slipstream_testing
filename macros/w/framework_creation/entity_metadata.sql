{% macro entity_metadata() %}
    
    {% set create_table %}
        
        create TABLE IF NOT EXISTS DBT_DEV.DBT_TCALANA_AUDIT.ENTITY_METADATA (
            ENTITY_ID NUMBER(38,0),
            LAYER_TYPE VARCHAR(100),
            DB_NAME VARCHAR(400),
            TABLE_TYPE VARCHAR(400),
            TABLE_NAME VARCHAR(400),
            TABLE_SCHEMA VARCHAR(400),
            HASH_COL_NAME VARCHAR(400),
            PKEY_COL_NAME VARCHAR(400),
            NKEY_COL_NAME VARCHAR(400),
            SKEY_COL_NAME VARCHAR(400),
            EFFECTIVE_DATE_COL_NAME VARCHAR(400),
            EXPIRATION_DATE_COL_NAME VARCHAR(400),
            PROCESS_TIME_COL_NAME VARCHAR(400),
            IS_ACTIVE_COL_NAME VARCHAR(400),
            unique (ENTITY_ID)
        );
    {% endset%}
    
    {% if execute %}
        {{ log('creating... ', info=True) }}
        {% do run_query(create_table) %}
        {{ log('Table DBT_DEV.DBT_TCALANA_AUDIT.ENTITY_METADATA created ' , info=True) }}
    {% endif %}
    
{% endmacro %}