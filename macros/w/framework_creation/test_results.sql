{% macro test_results() %}
    
    {% set create_table %}
        
       create TABLE IF NOT EXISTS DBT_DEV.DBT_TCALANA_AUDIT.TEST_RESULTS (
            RESULT_ID NUMBER(38,0),
            TEST_NAME VARCHAR(400),
            TEST_ID NUMBER(38,0),
            TEST_CONFIG_ID NUMBER(38,0),
            SRC_ENTITY VARCHAR(400),
            TRG_ENTITY VARCHAR(400),
            RESULT VARCHAR(50),
            RESULT_DESCRIPTION VARCHAR(400),
            TEST_TIME TIMESTAMP_NTZ(9),
            HWM_FROM TIMESTAMP_NTZ(9),
            HWM_TO TIMESTAMP_NTZ(9),
            unique (RESULT_ID),
            foreign key (TEST_ID) references DBT_DEV.DBT_TCALANA_AUDIT.TEST_METADATA(TEST_ID),
            foreign key (TEST_CONFIG_ID) references DBT_DEV.DBT_TCALANA_AUDIT.TEST_CONFIG(TEST_CONFIG_ID)
        );

    {% endset%}
    
    {% if execute %}
        {{ log('creating... ', info=True) }}
        {% do run_query(create_table) %}
        {{ log('Table DBT_DEV.DBT_TCALANA_AUDIT.TEST_RESULTS created ' , info=True) }}
    {% endif %}
    
{% endmacro %}