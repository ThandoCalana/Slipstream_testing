{% macro not_null_key(model_name, test_id, test_config_id, ts_col, keys) %}

  {% set test_name = 'not_null_key' %}
  {% set issues = [] %}

  {% set src_filtered = get_filtered_model(model_name, ts_col, 's') %}

  {% for key in keys %}
    {% set query %}
      SELECT COUNT(*) FROM {{ src_filtered }}
      WHERE {{ key }} IS NULL
    {% endset %}

    {% set result = run_query(query) %}
    {% set null_count = result.columns[0].values()[0] %}

    {% if null_count > 0 %}
      {% do issues.append(key ~ ' has ' ~ null_count ~ ' null(s)') %}
    {% endif %}
  {% endfor %}

  {% if issues | length == 0 %}
    {% set test_result = 'PASS' %}
    {% set result_description = 'All key columns have non-null values' %}
  {% else %}
    {% set test_result = 'FAIL' %}
    {% set result_description = issues | join('; ') %}
  {% endif %}

  {% do log_test_result(model_name, test_name, test_id, test_config_id, test_result, result_description) %}

{% endmacro %}
