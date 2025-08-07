{% set tables = generate_high_watermark_inserts('AIRBNB.TESTING.ENTITY_METADATA') %}

{% for table in tables %}
  {% call statement('insert_' ~ table, fetch_result=False) %}
    INSERT INTO AIRBNB.TESTING.HIGH_WATERMARK (TABLE_NAME, LAST_PROCESSED_TS)
    SELECT '{{ table }}', NULL
    WHERE NOT EXISTS (
        SELECT 1
        FROM AIRBNB.TESTING.HIGH_WATERMARK
        WHERE TABLE_NAME = '{{ table }}'
    );
  {% endcall %}
{% endfor %}
