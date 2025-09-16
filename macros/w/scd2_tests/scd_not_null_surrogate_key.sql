{% macro scd_not_null_surrogate_key(model_name, test_id, test_config_id, surrogate_key, ts_col) %}

  {% set test_name = 'scd_not_null_surrogate_key' %}
  
  {% set filtered = get_filtered_model(model_name, ts_col) %}
  {% set filtered_model = filtered[0] %}
  {% set hwm_to = filtered[1] %}
  {% set hwm_from = filtered[2] %}

  {% set query %}
    SELECT  
      {{ surrogate_key }}
    FROM {{ filtered_model }} 
    WHERE {{ surrogate_key }} IS NULL
  {% endset %}
  

  {% set result = run_query(query) %}
  {% set num_issues = result | length %}

  {% if num_issues == 2 %}
      {% set test_result = "PASS" %}
  {% else %}
      {% set test_result = "FAIL" %}
  {% endif %}

  {% set result_description = num_issues ~ ' null ' ~ surrogate_key ~' found' %}

  {% do log_test_result(model_name, test_name, test_id, test_config_id, test_result, result_description, None, hwm_from, hwm_to) %}
  
{% endmacro %}