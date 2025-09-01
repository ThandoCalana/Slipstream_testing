{% macro scd_not_null_active_natural_key(model_name, test_id, test_config_id, natural_key, ts_col, active_flag) %}

    {% set test_name = 'scd_not_null_active_natural_key' %}

    {% set filtered = get_filtered_model(model_name, ts_col) %}
    {% set filtered_model = filtered[0] %}
    {% set hwm_to = filtered[1] %}
    {% set hwm_from = filtered[2] %}

    {% set query %}
        SELECT *
        FROM {{ filtered_model }}
        WHERE {{ active_flag }} = TRUE
          AND {{ natural_key }} IS NULL
    {% endset %}

    {% set result = run_query(query) %}
    {% set inactives = result | length %}

    {% set test_result = 'PASS' if inactives == 0 else 'FAIL' %}

    {% set result_description = inactives ~ ' null active natural key values' %}    

    {% do log_test_result(model_name, test_name, test_id, test_config_id, test_result, result_description, None, hwm_from, hwm_to) %}

{% endmacro %}
