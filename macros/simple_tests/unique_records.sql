{% macro unique_records(model_name, model_schema, model_db, test_id, test_config_id, hash) %}

  {% set test_name = 'unique_records' %}
  {% set issues = [] %}
  {% set table_name = model_db ~ '.' ~ model_schema ~ '.' ~ model_name %}

  {% for key in keys %}
    {% set query %}
      SELECT COUNT(*) FROM (
        SELECT {{ hash }}, COUNT(*) AS cnt FROM {{ table_name }}
        GROUP BY {{ hash }}
        HAVING COUNT(*) > 1
      ) a
    {% endset %}

    {% set result = run_query(query) %}
    {% set duplicates = result.columns[0].values()[0] %}

    {% if duplicates > 0 %}
      {% do issues.append(key ~ ' has ' ~ duplicates ~ ' duplicate(s)') %}
    {% endif %}
  {% endfor %}

  {% if issues | length == 0 %}
    {% set test_result = 'PASS' %}
    {% set result_description = 'All hash values are unique' %}
  {% else %}
    {% set test_result = 'FAIL' %}
    {% set result_description = issues | join('; ') %}
  {% endif %}

  {% do log_test_result(model_name, test_name, test_id, test_config_id, test_result, result_description) %}

{% endmacro %}
