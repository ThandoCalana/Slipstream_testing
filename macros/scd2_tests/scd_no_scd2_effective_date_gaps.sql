{% macro scd_no_scd2_effective_date_gaps(model_name, test_id, test_config_id, skey) %}

    {% set test_name = 'scd_no_scd2_effective_date_gaps' %}

    {% set query %}
        SELECT * 
        FROM (
            SELECT {{ skey }},
                   EXPIRATION_DATE AS PREV_END,
                   LEAD(EFFECTIVE_DATE) OVER (PARTITION BY {{ skey }} ORDER BY EFFECTIVE_DATE) AS NEXT_START
            FROM {{ model_name }}
        ) GAP
        WHERE PREV_END IS NOT NULL 
        AND PREV_END != NEXT_START
    {% endset %}

    {% set result = run_query(query) %}
    {% set num_issues = result.columns[0].values() | length %}

    {% if num_issues == 0 %}
        {% set test_result = "PASS" %}
    {% else %}
        {% set test_result = "FAIL" %}
    {% endif %}

    {% set result_description = num_issues ~ ' effective date gaps found' %}

    {% do log_test_result(model_name, test_name, test_id, test_config_id, test_result, result_description) %}

{% endmacro %}
