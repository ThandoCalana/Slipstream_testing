{% macro row_count_match(src_model, test_id, test_config_id, trg_model, src_ts_col, trg_ts_col) %}

  {% set test_name = 'row_count_match' %}

  {% set src_filtered = get_filtered_model(src_model, src_ts_col, 's') %}
  {% set trg_filtered = get_filtered_model(trg_model, trg_ts_col, 't') %}

  {% set query %}
    SELECT
      (SELECT COUNT(*) FROM {{ src_filtered }}) AS src_count,
      (SELECT COUNT(*) FROM {{ trg_filtered }}) AS trg_count
  {% endset %}

  {% set result = run_query(query) %}
  {% set src_count = result.columns[0].values()[0] %}
  {% set trg_count = result.columns[1].values()[0] %}

  {% set test_result = 'PASS' if src_count == trg_count else 'FAIL' %}
  {% set result_description = 'Source count: ' ~ src_count ~ ', Target count: ' ~ trg_count %}

  {% do log_test_result(src_model, test_name, test_id, test_config_id, test_result, result_description, trg_model) %}

{% endmacro %}