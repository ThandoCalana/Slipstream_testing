{% macro get_filtered_model(src_model_name, src_timestamp_col_name, trg_model_name=None) %}

    {% if trg_model_name %}

        {% set hwm_from = get_latest_hwm(trg_model_name)[0] %}
        {% set hwm_to = get_latest_hwm(trg_model_name)[1] %}

    {% else %}

        {% set hwm_from = get_latest_hwm(src_model_name)[0] %}
        {% set hwm_to = get_latest_hwm(src_model_name)[1] %}
    
    {% endif %}


    {{ return([src_model_name ~
              " WHERE " ~ src_timestamp_col_name ~ " BETWEEN TO_TIMESTAMP_NTZ('" ~ hwm_from ~ "') AND " ~ "TO_TIMESTAMP_NTZ('" ~ hwm_to ~ "')", hwm_from, hwm_to]) }}
{% endmacro %}
