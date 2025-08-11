{% macro get_filtered_model(model_name, ts_col) %}

    {% set table_name = model_name.split('.')[-1] %}

    {% set last_ts = get_latest_hwm(table_name) %}
    {% set query %}
        SELECT *
        FROM {{ model_name }}
        WHERE {{ ts_col }} > '{{ last_ts }}'
    {% endset %}

    {% set result = run_query(query) %}

    {{return(result)}}

{% endmacro %}


