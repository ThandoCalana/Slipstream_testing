{% macro get_latest_hwm(model_name) %}
    {% set query %}
        SELECT PREVIOUS_PROCESS_TS, CURRENT_PROCESS_TS
        FROM AIRBNB.TESTING.HIGH_WATERMARK
        WHERE TABLE_NAME = '{{ model_name }}'
    {% endset %}

    {% set result = run_query(query) %}

    {% set previous_process_ts = result.columns[0].values()[0] %}
    {% set current_process_ts = result.columns[1].values()[0] %}

    {% if execute %}
        {% set current_process_ts = result.columns[1].values()[0] %}
        {% set previous_process_ts = result.columns[0].values()[0] %}
    {% else %}
        {% set current_process_ts = '1900-01-01 00:00:00' %}
        {% set previous_process_ts = '1900-01-01 00:00:00' %}
    {% endif %}
    
    {{ return([previous_process_ts, current_process_ts]) }}
{% endmacro %}

