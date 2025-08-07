{% macro get_latest_high_watermark(table_name) %}
    
    {% set query %}
        SELECT last_processed_ts
        FROM TESTING.HIGH_WATERMARK
        WHERE table_name = '{{ table_name }}'
    {% endset %}

    {% set result = run_query(query) %}
    {% if result.columns[0].values() %}
        {{ return(result.columns[0].values()[0]) }}
    {% else %}
        {{ return('1900-01-01 00:00:00') }}
    {% endif %}

{% endmacro %}
