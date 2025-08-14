{% macro scd_sequential_versions(model_name, test_id, test_config_id, skey) %}

    {% set test_name = 'scd_sequential_versions' %}

    {% set query %}
        SELECT *
        FROM (
            SELECT {{ skey }},
                   EFFECTIVE_DATE,
                   LAG(EFFECTIVE_DATE) OVER (PARTITION BY {{ skey }} ORDER BY EFFECTIVE_DATE) AS PREVIOUS_START
            FROM {{ model_name }}
        ) A
        WHERE PREVIOUS_START IS NOT NULL
          AND EFFECTIVE_DATE < PREVIOUS_START
    {% endset %}

    {% set result = run_query(query) %}
    {% set num_issues = result.columns[0].values() | length %}

    {% if num_issues == 0 %}
        {% set test_result = "PASS" %}
    {% else %}
        {% set test_result = "FAIL" %}
    {% endif %}

    {% set result_description = num_issues ~ ' non-sequential versions found' %}

    {% do log_test_result(model_name, test_name, test_id, test_config_id, test_result, result_description) %}

{% endmacro %}
