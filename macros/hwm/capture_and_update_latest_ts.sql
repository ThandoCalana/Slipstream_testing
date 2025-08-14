{% macro capture_and_update_latest_ts(model_name, ts_col) %}

{# Captures/stores the latest timestamp (ts) from the data currently being tested, and 
   also updates the HIGH_WATERMARK table with this new ts, for the correct table. #}

    {% set query %}
        SELECT MAX({{ ts_col }}) AS max_ts
        FROM {{ model_name }}
    {% endset %}

    {% set result = run_query(query) %}
    {% set max_ts = result.columns[0].values()[0] %}

    {% set table_parts = model_name.split(".") %}
    {% set table_name = table_parts[2] %}

    {% if max_ts is not none %}
        {% do update_hwm(table_name, max_ts) %}
    {% endif %}
   
{% endmacro %}
