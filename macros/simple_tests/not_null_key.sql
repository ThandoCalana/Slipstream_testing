{% macro not_null_key(model_name, model_schema, model_db, test_id, test_config_id, keys) %}

  {% set test_name = 'not_null_key' %}
  {% set issues = [] %}
  {% set table_name = model_db ~ '.' ~ model_schema ~ '.' ~ model_name %}

  {% for key in keys %}
    {% set query %}
      SELECT COUNT(*) FROM {{ table_name }}
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
