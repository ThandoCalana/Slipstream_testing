{% macro scd_inactive_historical_records(model_name, test_id, test_config_id) %}

    {% set test_name = 'scd_inactive_historical_records' %}

    {% set filtered = get_filtered_model(model_name, ts_col) %}
    {% set filtered_model = filtered[0] %}
    {% set current_process_ts = filtered[1] %}
    {% set previous_process_ts = filtered[2] %}

    {% set query %}
        SELECT *
        FROM {{ filtered_model }}
        WHERE IS_ACTIVE != TRUE
        AND EXPIRATION_DATE IS NULL
    {% endset %}

    {% set result = run_query(query) %}
    {% set num_issues = result | length %}

    {% if num_issues == 0 %}
        {% set test_result = "PASS" %}
    {% else %}
        {% set test_result = "FAIL" %}
    {% endif %}

    {% set result_description =  num_issues ~ ' active historical records' %}

    {% do log_test_result(model_name, test_name, test_id, test_config_id, test_result, result_description, None, previous_process_ts, current_process_ts) %}

{% endmacro %}
