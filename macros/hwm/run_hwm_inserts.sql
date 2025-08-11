{% macro run_hwm_inserts(metadata_table) %}
    {% set results = run_query("SELECT DISTINCT table_name FROM " ~ metadata_table ~ " WHERE layer_type IN ('LANDING', 'LAKE', 'WAREHOUSE')") %}
    {% set table_names = results.columns[0].values() %}

    {% for table in table_names %}
        {% do run_query(
            "INSERT INTO AIRBNB.TESTING.HIGH_WATERMARK (TABLE_NAME, PREVIOUS_PROCESS_TS, CURRENT_PROCESS_TS)
             SELECT '" ~ table ~ "', NULL, NULL
             WHERE NOT EXISTS (
                 SELECT 1
                 FROM AIRBNB.TESTING.HIGH_WATERMARK
                 WHERE TABLE_NAME = '" ~ table ~ "'
             );"
        ) %}
    {% endfor %}
{% endmacro %}
