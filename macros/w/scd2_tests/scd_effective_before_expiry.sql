{% macro scd_effective_before_expiry(model_name, natural_key, test_id, test_config_id, ts_col, effective_date, expiration_date) %}

    {% set test_name = 'scd_effective_before_expiry' %}

    {% set filtered = get_filtered_model(model_name, ts_col) %}
    {% set filtered_model = filtered[0] %}
    {% set hwm_to = filtered[1] %}
    {% set hwm_from = filtered[2] %}

    {% set query %}
        SELECT {{ natural_key }}, {{ effective_date }}, {{ expiration_date }}
        FROM {{ filtered_model }}
        WHERE {{ effective_date }} > {{ expiration_date }}
        AND {{ expiration_date }} IS NOT NULL
    {% endset %}

    {% set result = run_query(query) %}
    {% set misordered_dates = result | length %}

    {% if misordered_dates == 0 %}
        {% set test_result = "PASS" %}
    {% else %}
        {% set test_result = "FAIL" %}
    {% endif %}

    {% set result_description =  misordered_dates ~ ' misordered dates found' %}

    {% do log_test_result(model_name, test_name, test_id, test_config_id, test_result, result_description, None, hwm_from, hwm_to) %}

{% endmacro %}
