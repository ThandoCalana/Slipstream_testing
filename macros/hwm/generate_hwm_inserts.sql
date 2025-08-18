{% macro generate_high_watermark_inserts(metadata_table) %}
    {% set results = run_query("SELECT DISTINCT table_name FROM " ~ metadata_table ~ " WHERE layer_type IN ('LANDING', 'LAKE', 'WAREHOUSE')") %}
    {% set table_names = results.columns[0].values() %}
    
    {{ return(table_names) }}

{% endmacro %}
