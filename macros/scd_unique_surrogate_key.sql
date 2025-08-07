{% macro scd_unique_surrogate_key(model_name, test_id, test_config_id, key) %}

    {% set test_name = 'scd_unique_surrogate_key' %}

    {% set query %}
        SELECT {{ key }},
        COUNT({{ key }}) AS key_count
        FROM {{ model_name }}
        GROUP BY {{ key }}
        HAVING key_count > 1
    {% endset %}

    {% set result = run_query(query) %}
    {% set num_issues = result.columns[0].values() | length %}

    {% if num_issues == 0 %}
        {% set test_result = "PASS" %}
    {% else %}
        {% set test_result = "FAIL" %}
    {% endif %}

    {% set result_description = num_issues ~ ' duplicate ' ~ key %}

    {% do log_test_result(model_name, test_name, test_id, test_config_id, test_result, result_description) %}

{% endmacro %}
