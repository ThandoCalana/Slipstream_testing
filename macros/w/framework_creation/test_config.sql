{% macro test_config() %}
    
    {% set create_table %}
        
        create TABLE IF NOT EXISTS DBT_DEV.DBT_TCALANA_AUDIT.TEST_CONFIG (
            TEST_CONFIG_ID NUMBER(38,0),
            TEST_ID NUMBER(38,0),
            SRC_ENTITY_ID NUMBER(38,0),
            TRG_ENTITY_ID NUMBER(38,0),
            IS_ACTIVE BOOLEAN,
            TEST_FREQUENCY VARCHAR(400),
            unique (TEST_CONFIG_ID),
            foreign key (TEST_ID) references DBT_DEV.DBT_TCALANA_AUDIT.TEST_METADATA(TEST_ID),
            foreign key (SRC_ENTITY_ID) references DBT_DEV.DBT_TCALANA_AUDIT.ENTITY_METADATA(ENTITY_ID),
            foreign key (TRG_ENTITY_ID) references DBT_DEV.DBT_TCALANA_AUDIT.ENTITY_METADATA(ENTITY_ID)
        );

    {% endset%}
    
    {% if execute %}
        {{ log('creating... ', info=True) }}
        {% do run_query(create_table) %}
        {{ log('Table DBT_DEV.DBT_TCALANA_AUDIT.TEST_CONFIG created ' , info=True) }}
    {% endif %}
    
{% endmacro %}