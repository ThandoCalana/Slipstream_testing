{% macro unique_records(model_name, test_id, test_config_id, hash_col, ts_col) %}

  {% set test_name = 'unique_hashes' %}
  {% set issues = [] %}

  {% set src_filtered = get_filtered_model(model_name, ts_col, 's') %}

  {% set query %}
    SELECT COUNT(*) FROM (
      SELECT {{ hash_col }}, COUNT(*) AS cnt FROM {{ src_filtered }}
      GROUP BY {{ hash_col }}
      HAVING COUNT(*) > 1
    ) a
  {% endset %}

  {% set result = run_query(query) %}
  {% set duplicates = result.columns[0].values()[0] %}
  {% set test_result = 'PASS' if duplicates == 0 else 'FAIL' %} 
  {% set result_description = 'Duplicate hash values: ' ~ duplicates %}

  {% do log_test_result(model_name, test_name, test_id, test_config_id, test_result, result_description) %}

{% endmacro %}
