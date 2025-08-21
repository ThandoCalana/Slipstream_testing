{% macro capture_and_update_latest_ts(model_name, full_table_name, ts_col) %}

{# Captures/stores the latest timestamp (ts) from the data currently being tested, and 
   also updates the HIGH_WATERMARK table with this new ts, for the correct table. #}
   
    {% set query %}
        SELECT MAX({{ ts_col }}) AS max_ts
        FROM {{ model_name }}
    {% endset %}

    {% set result = run_query(query) %}
    {% set new_timestamp_value = result.columns[0].values()[0] %}
    

    {% set table_parts = full_table_name.split(".") %}
    {% set table_name = table_parts[2] %}


    {% if new_timestamp_value is not none %}
        {% do update_hwm(table_name, new_timestamp_value) %}
    {% endif %}
   
{% endmacro %}
