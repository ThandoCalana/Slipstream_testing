{% macro scd_not_null_active_natural_key(model_name, test_id, test_config_id, key) %}

    {% set test_name = 'scd_not_null_active_natural_key' %}

    {% set query %}
        SELECT *
        FROM {{ model_name }}
        WHERE IS_ACTIVE = TRUE
          AND {{ key }} IS NULL
    {% endset %}

    {% set result = run_query(query) %}
    {% set inactives = result | length %}

    {% set test_result = 'PASS' if inactives == 0 else 'FAIL' %}

    {% set result_description = inactives ~ ' null active natural key values' %}    

    {% do log_test_result(model_name, test_name, test_id, test_config_id, test_result, result_description) %}

{% endmacro %}
