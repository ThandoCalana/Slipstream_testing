{% macro get_filtered_model(model_name, ts_col, alias) %}
    {% set table_parts = model_name.split(".") %}
    {% set table_name = table_parts[2] %}
    {% set last_ts = get_latest_hwm(table_name) %}


    {{ return("(SELECT * FROM " ~ model_name ~
              " WHERE " ~ ts_col ~ " > TO_TIMESTAMP_NTZ('" ~ last_ts ~ "')) " ~ alias) }}
{% endmacro %}
