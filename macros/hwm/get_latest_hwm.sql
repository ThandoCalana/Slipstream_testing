{% macro get_latest_hwm(model_name) %}
    {% set query %}
        SELECT HWM_FROM, HWM_TO
        FROM AIRBNB.TESTING.HIGH_WATERMARK
        WHERE TABLE_NAME = '{{ model_name }}'
    {% endset %}

    {% set result = run_query(query) %}

    {% set hwm_from = result.columns[0].values()[0] %}
    {% set hwm_to = result.columns[1].values()[0] %}

    {% if execute %}
        {% set hwm_to = result.columns[1].values()[0] %}
        {% set hwm_from = result.columns[0].values()[0] %}
    {% else %}
        {% set hwm_to = '1900-01-01 00:00:00' %}
        {% set hwm_from = '1900-01-01 00:00:00' %}
    {% endif %}
    
    {{ return([hwm_from, hwm_to]) }}
{% endmacro %}

