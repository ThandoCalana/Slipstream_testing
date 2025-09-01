{% macro update_hwm(table_name, new_timestamp_value) %}

{# Updates the previous and current timestamp (ts) columns of the HIGH_WATERMARK table,
    with the relevant timestamps, of the table which tests are being ran on. On initial load,
    the timestamp will be defaulted to 1900-01-01 00:00:00 so that all the data can be tested. #}

    {% set query %}
        MERGE INTO AIRBNB.TESTING.HIGH_WATERMARK AS target
        USING (
            SELECT
                '{{ table_name }}' AS table_name,
                target.HWM_TO AS HWM_FROM,
                '{{ new_timestamp_value }}'::TIMESTAMP_NTZ AS HWM_TO
            FROM AIRBNB.TESTING.HIGH_WATERMARK target
            WHERE table_name = '{{ table_name }}'
        ) AS source
        ON target.table_name = source.table_name
        WHEN MATCHED THEN
            UPDATE SET
                HWM_FROM = target.HWM_TO,
                HWM_TO = source.HWM_TO
        WHEN NOT MATCHED THEN
            INSERT (table_name, HWM_FROM, HWM_TO)
            VALUES ('{{ table_name }}', '1900-01-01 00:00:00', '{{ new_timestamp_value }}')
    {% endset %}

    {% do run_query(query) %}

{% endmacro %}
