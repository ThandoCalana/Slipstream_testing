{% macro log_test_result(src_model, test_name, test_id, test_config_id, result, result_description, trg_model=None, hwm_from=None, hwm_to=None) %}

  {% set result_id = (
    modules.datetime.datetime.now().isoformat()
    | replace("-", "")
    | replace(":", "")
    | replace("T", "")
    | replace(".", "")
    | truncate(18, True)
  ) %}

  {% set hwm_from_val = "'" ~ hwm_from ~ "'" if hwm_from else "NULL" %}
  {% set hwm_to_val   = "'" ~ hwm_to ~ "'"   if hwm_to   else "NULL" %}

  {% set insert_statement %}
    INSERT INTO DBT_DEV.DBT_TCALANA_AUDIT.TEST_RESULTS (
      RESULT_ID, TEST_NAME, TEST_ID, TEST_CONFIG_ID, SRC_ENTITY, TRG_ENTITY, RESULT,
      RESULT_DESCRIPTION, TEST_TIME, HWM_FROM, HWM_TO
    )
    VALUES (
      '{{ result_id }}',
      '{{ test_name }}',
      {{ test_id }},
      {{ test_config_id }},
      '{{ src_model }}',
      '{{ trg_model }}',
      '{{ result }}',
      '{{ result_description }}',
      CURRENT_TIMESTAMP,
      {{ hwm_from_val }},
      {{ hwm_to_val }}
    )
  {% endset %}

  {% do run_query(insert_statement) %}
  {% do log("Logged result for test ID " ~ test_id ~ " / config " ~ test_config_id ~ ": " ~ result_description, info=True) %}

{% endmacro %}
