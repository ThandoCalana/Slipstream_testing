{% macro get_filtered_data_for_testing(model_name, ts_col) %}
    
    {% set last_ts = get_latest_high_watermark(model_name) %}

    SELECT * 
    FROM {{ model_name }}
    WHERE {{ ts_col }} > '{{ last_ts }}'

{% endmacro %}
