{% macro get_latest_hwm(model_name) %}
{# Get the latest high watermark period for the given input table
    by querying the high_watermark table. #}

    {% set query %}
        SELECT HWM_FROM, HWM_TO
        FROM AIRBNB.TESTING.HIGH_WATERMARK
        WHERE TABLE_NAME = '{{ model_name }}'
    {% endset %}

    {% set result = run_query(query) %}

    {% set hwm_from = result.columns[0].values()[0] %}
    {% set hwm_to = result.columns[1].values()[0] %}
    
    {{ return([hwm_from, hwm_to]) }}
{% endmacro %}

