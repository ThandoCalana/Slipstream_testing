{% macro capture_and_update_latest_ts(src_table_name, src_ts_col, trg_table_name=None, trg_ts_col=None) %}

{# Captures/stores the latest timestamp (ts) from the data currently being tested, and 
   also updates the HIGH_WATERMARK table with this new ts, for the correct table. #}
   
   {% if trg_table_name %}
    {# If there's a target table, we use its max date as this will confirm the max date of data that
    has finished being processed. #}

        {% set query %}
            SELECT MAX({{ trg_ts_col }}) AS max_ts
            FROM {{ trg_table_name }}
        {% endset %}

   {% else %}

        {% set query %}
            SELECT MAX({{ src_ts_col }}) AS max_ts
            FROM {{ src_table_name }}
        {% endset %}

   {% endif %}

    {% set result = run_query(query) %}
    {% set new_timestamp_value = result.columns[0].values()[0] %}
    

    {% do update_hwm(src_table_name, new_timestamp_value) %}
   
{% endmacro %}
