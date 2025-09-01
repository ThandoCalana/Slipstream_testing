{% macro scd_no_scd2_effective_date_gaps(model_name, test_id, test_config_id, natural_key, ts_col, effective_date, expiration_date) %}

    {% set test_name = 'scd_no_scd2_effective_date_gaps' %}

    {% set filtered = get_filtered_model(model_name, ts_col) %}
    {% set filtered_model = filtered[0] %}
    {% set hwm_to = filtered[1] %}
    {% set hwm_from = filtered[2] %}

    {% set query %}
        SELECT * 
        FROM (
            SELECT {{ natural_key }},
                   {{ expiration_date }} AS PREV_END,
                   LEAD( {{ effective_date }} ) OVER ( PARTITION BY {{ natural_key }} ORDER BY {{ effective_date }} ) AS NEXT_START
            FROM {{ filtered_model }}
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

    {% do log_test_result(model_name, test_name, test_id, test_config_id, test_result, result_description, None, hwm_from, hwm_to) %}

{% endmacro %}
