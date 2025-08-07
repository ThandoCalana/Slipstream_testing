{% macro scd_unique_active_nkey(model_name, test_id, test_config_id, nkey) %}

    {% set test_name = 'scd_unique_active_nkey' %}

    {% set query %}
        SELECT {{ nkey }}
        FROM {{ model_name }}
        WHERE IS_ACTIVE = TRUE
        GROUP BY {{ nkey }}
        HAVING COUNT({{ nkey }}) > 1
    {% endset %}

    {% set result = run_query(query) %}
    {% set num_issues = result | length %}

    {% if num_issues == 0 %}
        {% set test_result = "PASS" %}
    {% else %}
        {% set test_result = "FAIL" %}
    {% endif %}

    {% set result_description = num_issues ~ ' customers with multiple active records' %}

    {% do log_test_result(model_name, test_name, test_id, test_config_id, test_result, result_description) %}

{% endmacro %}
