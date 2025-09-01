{% macro capture_and_update_latest_ts(full_table_name, ts_col) %}

{# Captures/stores the latest timestamp (ts) from the data currently being tested, and 
   also updates the HIGH_WATERMARK table with this new ts, for the correct table. #}
   
    {% set query %}
        SELECT MAX({{ ts_col }}) AS max_ts
        FROM {{ full_table_name }}
    {% endset %}

    {% set result = run_query(query) %}
    {% set new_timestamp_value = result.columns[0].values()[0] %}
    

    {% set table_parts = full_table_name.split(".") %}
    {% set table_name = table_parts[2] %}

    {% do update_hwm(table_name, new_timestamp_value) %}
   
{% endmacro %}
