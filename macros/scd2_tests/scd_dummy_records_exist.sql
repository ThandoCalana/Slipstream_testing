{% macro scd_dummy_records_exist(model_name, model_schema, db, primary_key, test_id, test_config_id) %}

  {% set test_name = 'scd_dummy_records_exist' %}

  {% set src_table = db ~ '.' ~ model_schema ~ '.' ~ model_name %}

  {% set query %}
    SELECT COUNT(DISTINCT {{ primary_key }}) AS dummy_count
    FROM {{ src_table }}
    WHERE {{ primary_key }} IN ('N/A', 'NOT_FOUND')
  {% endset %}

  {% set result = run_query(query) %}
  {% set dummy_count = result.columns[0].values()[0] %}
  {% set test_result = 'PASS' if dummy_count == 2 else 'FAIL' %}
  {% set result_description = 'Dummy records found: ' ~ dummy_count ~ ' (expected 2)' %}

  {% do log_test_result(model_name, test_name, test_id, test_config_id, test_result, result_description) %}

{% endmacro %}
