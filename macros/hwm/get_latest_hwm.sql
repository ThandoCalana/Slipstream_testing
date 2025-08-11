{% macro get_latest_hwm(model_name) %}

    {% set query %}
        SELECT current_process_ts
        FROM AIRBNB.TESTING.HIGH_WATERMARK
        WHERE table_name = '{{ model_name }}'
    {% endset %}

    {% set result = run_query(query) %}
    {% if result.columns[0].values() %}
        {{ return(result.columns[0].values()[0]) }}
    {% else %}
        {{ return('1900-01-01 00:00:00') }}
    {% endif %}

    {{log('Results: '~result, info=True)}}

    {{ return(result) }}
    
{% endmacro %}
