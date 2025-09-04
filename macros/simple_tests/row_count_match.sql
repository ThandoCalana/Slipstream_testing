{% macro row_count_match(src_model, test_id, test_config_id, trg_model, src_ts_col, trg_ts_col) %}

  {% set test_name = 'row_count_match' %}

  {% set src_filtered = get_filtered_model(src_model, src_ts_col)[0] %}
  {% set trg_filtered = get_filtered_model(trg_model, trg_ts_col)[0] %}


  {% set query %}
    SELECT
  (SELECT COUNT(*) 
   FROM (SELECT * FROM {{ src_filtered }}) AS s) AS src_count,
  (SELECT COUNT(*) 
   FROM (SELECT * FROM {{ trg_filtered }}) AS t) AS trg_count;
  
  {% endset %}

  {% set result = run_query(query) %}
  {% set src_count = result.columns[0].values()[0] %}
  {% set trg_count = result.columns[1].values()[0] %}

  {% set test_result = 'PASS' if src_count == trg_count else 'FAIL' %}
  {% set result_description = 'Source count: ' ~ src_count ~ ', Target count: ' ~ trg_count %}

  {% set hwm_from = get_filtered_model(src_model, src_ts_col)[1] %}
  {% set hwm_to = get_filtered_model(src_model, src_ts_col)[2] %}


  {% do log_test_result(src_model, test_name, test_id, test_config_id, test_result, result_description, trg_model, hwm_from, hwm_to) %}

{% endmacro %}