{% macro scd_unique_active_nkey(model_name, test_id, test_config_id, natural_key, ts_col) %}

    {% set test_name = 'scd_unique_active_nkey' %}

    {% set filtered = get_filtered_model(model_name, ts_col) %}
    {% set filtered_model = filtered[0] %}
    {% set hwm_to = filtered[1] %}
    {% set hwm_from = filtered[2] %}

    {% set query %}
        SELECT {{ natural_key }}
        FROM {{ filtered_model }}
        WHERE IS_ACTIVE = TRUE
        GROUP BY {{ natural_key }}
        HAVING COUNT({{ natural_key }}) > 1
    {% endset %}

    {% set result = run_query(query) %}
    {% set num_issues = result | length %}

    {% if num_issues == 0 %}
        {% set test_result = "PASS" %}
    {% else %}
        {% set test_result = "FAIL" %}
    {% endif %}

    {% set result_description = num_issues ~ ' customers with multiple active records' %}

    {% do log_test_result(model_name, test_name, test_id, test_config_id, test_result, result_description, None, hwm_from, hwm_to) %}

{% endmacro %}
