{% macro update_high_watermark(table_name, new_ts) %}
    
    {% set query %}
        MERGE INTO TESTING.HIGH_WATERMARK AS target
        USING (SELECT '{{ table_name }}' AS table_name, '{{ new_ts }}'::TIMESTAMP_NTZ AS last_processed_ts) AS source
        ON target.table_name = source.table_name
        WHEN MATCHED THEN UPDATE SET last_processed_ts = source.last_processed_ts
        WHEN NOT MATCHED THEN INSERT (table_name, last_processed_ts) 
        VALUES (source.table_name, source.last_processed_ts)
    {% endset %}

    {% do run_query(query) %}

{% endmacro %}
