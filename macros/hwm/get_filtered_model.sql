{% macro get_filtered_model(model_name, ts_col) %}
    {% set table_parts = model_name.split(".") %}
    {% set table_name = table_parts[2] %}
    {% set current_process_ts = get_latest_hwm(table_name)[1] %}
    {% set previous_process_ts = get_latest_hwm(table_name)[0] %}


    {{ return(["(SELECT * FROM " ~ model_name ~
              " WHERE " ~ ts_col ~ " > TO_TIMESTAMP_NTZ('" ~ current_process_ts ~ "')) ", current_process_ts, previous_process_ts]) }}
{% endmacro %}
