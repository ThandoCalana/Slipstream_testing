{% macro capture_and_update_latest_ts(model_name, ts_col) %}
    
    {% set query %}
        SELECT MAX({{ ts_col }}) AS max_ts
        FROM {{ model_name }}
    {% endset %}

    {% set result = run_query(query) %}
    {% set max_ts = result.columns[0].values()[0] %}
    

    
    {% if max_ts is not none %}
        {% do update_hwm(model_name, max_ts) %}
    {% endif %}
    
{% endmacro %}
