{% macro hash_match(src_model, src_schema, src_db, src_pkey, src_hash_col, src_load_timestamp, test_id, test_config_id, trg_model, trg_schema, trg_db, trg_pkey, trg_hash_col, trg_load_timestamp) %}

  {% set test_name = 'hash_match' %}
  {% set src_table = src_db ~ '.' ~ src_schema ~ '.' ~ src_model %}
  {% set trg_table = trg_db ~ '.' ~ trg_schema ~ '.' ~ trg_model %}

  {% set query %}
    SELECT COUNT(*) AS mismatch_count
    FROM {{ src_table }} s
    INNER JOIN {{ trg_table }} t
      ON s.{{ src_pkey }} = t.{{ trg_pkey }}
    WHERE (s.{{ src_load_timestamp }} = t.{{ trg_load_timestamp }} AND (
      (s.{{ src_hash_col }} IS NULL AND t.{{ trg_hash_col }} IS NOT NULL) OR
      (s.{{ src_hash_col }} IS NOT NULL AND t.{{ trg_hash_col }} IS NULL) OR
      (s.{{ src_hash_col }} != t.{{ trg_hash_col }}))
    )
  {% endset %}

  {% set result = run_query(query) %}
  {% set mismatches = result.columns[0].values()[0] %}
  {% set test_result = 'PASS' if mismatches == 0 else 'FAIL' %}
  {% set result_description = 'Row-level hash mismatches: ' ~ mismatches %}

  {% do log_test_result(src_model, test_name, test_id, test_config_id, test_result, result_description, trg_model) %}

{% endmacro %}
