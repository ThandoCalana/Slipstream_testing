{% macro test_metadata() %}
    
    {% set create_table %}
        
        create TABLE IF NOT EXISTS DBT_DEV.DBT_TCALANA_AUDIT.TEST_METADATA (
            TEST_ID NUMBER(38,0),
            TEST_NAME VARCHAR(400),
            TEST_TYPE VARCHAR(400),
            TEST_DESCRIPTION VARCHAR(400),
            TEST_FREQUENCY VARCHAR(100),
            unique (TEST_ID)
        );

    {% endset%}
    
    {% if execute %}
        {{ log('creating... ', info=True) }}
        {% do run_query(create_table) %}
        {{ log('Table DBT_DEV.DBT_TCALANA_AUDIT.TEST_METADATA created ' , info=True) }}
    {% endif %}
    
{% endmacro %}