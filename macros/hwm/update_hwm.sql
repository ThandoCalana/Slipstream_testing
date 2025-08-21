{% macro update_hwm(table_name, new_timestamp_value) %}

{# Updates the previous and current timestamp (ts) columns of the HIGH_WATERMARK table,
    with the relevant timestamps, of the table which tests are being ran on. On initial load,
    the timestamp will be defaulted to 1900-01-01 00:00:00 so that all the data can be tested. #}

    {% set query %}
        MERGE INTO AIRBNB.TESTING.HIGH_WATERMARK AS target
        USING (
            SELECT
                '{{ table_name }}' AS table_name,
                target.current_process_ts AS previous_process_ts,
                '{{ new_timestamp_value }}'::TIMESTAMP_NTZ AS current_process_ts
            FROM AIRBNB.TESTING.HIGH_WATERMARK target
            WHERE table_name = '{{ table_name }}'
        ) AS source
        ON target.table_name = source.table_name
        WHEN MATCHED THEN
            UPDATE SET
                previous_process_ts = target.current_process_ts,
                current_process_ts = source.current_process_ts
        WHEN NOT MATCHED THEN
            INSERT (table_name, previous_process_ts, current_process_ts)
            VALUES ('{{ table_name }}', '1900-01-01 00:00:00', '{{ new_timestamp_value }}')
    {% endset %}

    {% do run_query(query) %}

{% endmacro %}
