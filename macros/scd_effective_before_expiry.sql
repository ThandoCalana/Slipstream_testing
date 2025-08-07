{% macro scd_effective_before_expiry(model_name, test_id, test_config_id) %}

    {% set test_name = 'scd_effective_before_expiry' %}

    {% set query %}
        SELECT CUSTOMER_ID, EFFECTIVE_DATE, EXPIRATION_DATE
        FROM {{ model_name }}
        WHERE EFFECTIVE_DATE > EXPIRATION_DATE
        AND EXPIRATION_DATE IS NOT NULL
    {% endset %}

    {% set result = run_query(query) %}
    {% set misordered_dates = result | length %}

    {% if misordered_dates == 0 %}
        {% set test_result = "PASS" %}
    {% else %}
        {% set test_result = "FAIL" %}
    {% endif %}

    {% set result_description =  misordered_dates ~ ' misordered dates found' %}

    {% do log_test_result(model_name, test_name, test_id, test_config_id, test_result, result_description) %}

{% endmacro %}
