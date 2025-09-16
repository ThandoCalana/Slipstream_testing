{% macro high_watermark() %}
    
    {% set create_table %}
        
        create TABLE IF NOT EXISTS DBT_DEV.DBT_TCALANA_AUDIT.HIGH_WATERMARK (
            TABLE_KEY NUMBER(38,0),
            TABLE_NAME VARCHAR(100),
            HWM_FROM TIMESTAMP_NTZ(9),
            HWM_TO TIMESTAMP_NTZ(9),
            foreign key (TABLE_KEY) references DBT_DEV.DBT_TCALANA_AUDIT.ENTITY_METADATA(ENTITY_ID)
);
    {% endset%}
    
    {% if execute %}
        {{ log('creating... ', info=True) }}
        {% do run_query(create_table) %}
        {{ log('Table DBT_DEV.DBT_TCALANA_AUDIT.HIGH_WATERMARK created ' , info=True) }}
    {% endif %}
    
{% endmacro %}