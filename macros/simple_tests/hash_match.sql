{% macro hash_match(src_model, src_pkey, src_hash_col, test_id, test_config_id,
                    trg_model, trg_pkey, trg_hash_col, src_ts_col, trg_ts_col) %}

  {% set test_name = 'hash_match' %}

  {% set src_filtered = get_filtered_model(src_model, src_ts_col) %}
  {% set trg_filtered = get_filtered_model(trg_model, trg_ts_col) %}

  {% set query %}
    SELECT COUNT(*) AS mismatch_count
    FROM {{ src_filtered }} AS s
    INNER JOIN {{ trg_filtered }} AS s
      ON s.{{ src_pkey }} = t.{{ trg_pkey }}
    WHERE
      (s.{{ src_hash_col }} IS NULL AND t.{{ trg_hash_col }} IS NOT NULL) OR
      (s.{{ src_hash_col }} IS NOT NULL AND t.{{ trg_hash_col }} IS NULL) OR
      (s.{{ src_hash_col }} != t.{{ trg_hash_col }})
  {% endset %}

  {% set result = run_query(query) %}
  {% set mismatches = result.columns[0].values()[0] %}
  {% set test_result = 'PASS' if mismatches == 0 else 'FAIL' %}
  {% set result_description = 'Row-level hash mismatches: ' ~ mismatches %}

  {% do log_test_result(src_model, test_name, test_id, test_config_id,
                        test_result, result_description, trg_model) %}
{% endmacro %}
