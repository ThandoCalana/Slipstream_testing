{% macro get_filtered_model(model_name, timestamp_name) %}

    {% set table_parts = model_name.split(".") %}
    {% set table_name = table_parts[2] %}

    {% set hwm_from = get_latest_hwm(table_name)[0] %}
    {% set hwm_to = get_latest_hwm(table_name)[1] %}

    
    {{ return([model_name ~
              " WHERE " ~ timestamp_name ~ " BETWEEN TO_TIMESTAMP_NTZ('" ~ hwm_from ~ "') AND " ~ " TO_TIMESTAMP_NTZ('" ~ hwm_to ~ "')", hwm_from, hwm_to]) }}
{% endmacro %}
