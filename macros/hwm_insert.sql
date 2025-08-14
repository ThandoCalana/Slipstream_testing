{% macro hwm_insert(metadata_table) %}

  {# Generates the insert statements to populate the HIGH_WATERMARK table
  with the relevant tables, taken fromt the ENTITY_METADATA table. #}

  {% set query %}
    SELECT DISTINCT TABLE_NAME
    FROM {{ metadata_table }}
    WHERE LAYER_TYPE IN ('LANDING', 'LAKE', 'WAREHOUSE')
  {% endset %}

  {% set results = run_query(query) %}
  {% set table_names = results.columns[0].values() %}

  {% if table_names | length > 0 %}
    {% set insert_sql %}
      INSERT INTO AIRBNB.TESTING.HIGH_WATERMARK (TABLE_NAME, PREVIOUS_PROCESS_TS, CURRENT_PROCESS_TS)
      SELECT t.table_name, NULL, NULL
      FROM (
        {% for table in table_names %}
          SELECT '{{ table }}' AS table_name
          {% if not loop.last %} UNION ALL {% endif %}
        {% endfor %}
      ) t
      LEFT JOIN AIRBNB.TESTING.HIGH_WATERMARK hw
        ON t.table_name = hw.table_name
      WHERE hw.table_name IS NULL
    {% endset %}

    {% do run_query(insert_sql) %}
  {% endif %}

{% endmacro %}
