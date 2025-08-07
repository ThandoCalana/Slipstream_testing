{% macro scd_initial_effective_date_set(model_name, test_id, test_config_id) %}

    {% set test_name = 'scd_initial_effective_date_set' %}

    {% set query %}
        SELECT *
        FROM {{ model_name }}
        WHERE EFFECTIVE_DATE IS NULL
    {% endset %}

    {% set result = run_query(query) %}
    {% set num_issues = result | length %}

    {% if num_issues == 0 %}
        {% set test_result = "PASS" %}
    {% else %}
        {% set test_result = "FAIL" %}
    {% endif %}

    {% set result_description = num_issues ~ ' records missing effective date' %}

    {% do log_test_result(model_name, test_name, test_id, test_config_id, test_result, result_description) %}

{% endmacro %}
