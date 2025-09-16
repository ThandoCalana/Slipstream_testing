{% macro column_presence_and_types(src_model, src_schema, test_id, test_config_id, trg_model, trg_schema) %}

  {% set test_name = 'column_presence_and_types' %}

  {% set query %}
    SELECT COUNT(*) AS mismatch_count FROM (
      SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.columns
      WHERE TABLE_NAME = UPPER('{{ src_model }}') AND TABLE_SCHEMA = UPPER('{{ src_schema }}')
      MINUS
      SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.columns
      WHERE TABLE_NAME = UPPER('{{ trg_model }}') AND TABLE_SCHEMA = UPPER('{{ trg_schema }}')
    ) a
  {% endset %}

  {% set result = run_query(query) %}
  {% set mismatch_count = result.columns[0].values()[0] %}
  {% set test_result = 'PASS' if mismatch_count == 0 else 'FAIL' %}
  {% set result_description = 'Column/type mismatches found: ' ~ mismatch_count %}

  {% do log_test_result(src_model, test_name, test_id, test_config_id, test_result, result_description, trg_model) %}

{% endmacro %}