{% macro scd_columns_check(model_name, test_id, test_config_id, model_schema) %}
  
  {% set test_name = 'scd_columns_check' %}

  {% set query %}

    SELECT * FROM information_schema.columns
    WHERE UPPER(table_name) = UPPER('{{ model_name }}')
      AND UPPER(table_schema) = UPPER('{{ model_schema }}')
      AND UPPER(column_name) IN ('EFFECTIVE_DATE', 'EXPIRATION_DATE', 'CUSTOMER_SK' )
      
  {% endset %}

  {% set result = run_query(query) %}
  {% set count = result.columns[0].values() | length %}


  {% if count == 3 %}
        {% set test_result = "PASS" %}
    {% else %}
        {% set test_result = "FAIL" %}
  {% endif %}

  {% set result_description = 'Required SCD2 columns present: ' ~ count %}

  {% do log_test_result(model_name, test_name, test_id, test_config_id, test_result, result_description) %}

{% endmacro %}
