{% macro get_latest_hwm(model_name) %}
    {% set query %}
        SELECT CURRENT_PROCESS_TS
        FROM AIRBNB.TESTING.HIGH_WATERMARK
        WHERE TABLE_NAME = '{{ model_name }}'
    {% endset %}

    {% set result = run_query(query) %}

    {% if execute %}
        {% set last_ts = result.columns[0].values()[0] %}
    {% else %}
        {% set last_ts = '1900-01-01 00:00:00' %}
    {% endif %}
    
    {{ return(last_ts) }}
{% endmacro %}

