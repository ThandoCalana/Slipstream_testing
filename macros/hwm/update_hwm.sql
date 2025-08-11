{% macro update_hwm(table_name, new_ts) %}

    {% set query %}
        MERGE INTO AIRBNB.TESTING.HIGH_WATERMARK AS target
        USING (
            SELECT
                '{{ table_name }}' AS table_name,
                target.current_process_ts AS previous_process_ts,
                '{{ new_ts }}'::TIMESTAMP_NTZ AS current_process_ts
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
            VALUES ('{{ table_name }}', '1900-01-01 00:00:00', '{{ new_ts }}')
    {% endset %}

    {% do run_query(query) %}

{% endmacro %}
