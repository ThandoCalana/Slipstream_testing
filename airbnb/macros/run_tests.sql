{% macro run_tests() %}

-- Generate Test Run ID
{% set run_id_query = run_query("SELECT UUID_STRING()") %}
{% set test_run_id = run_id_query.columns[0][0] %}

{{ log('Starting test run: ' ~ test_run_id, info=True) }}

-- Fetch active tests from the config
{% set results = run_query(
    "SELECT 
        test_id,
        test_name,
        test_type,
        source_table_name,
        target_table_name,
        surrogate_key
     FROM AIRBNB.DEV.TEST_CONFIG
     WHERE active_flag = TRUE"
) %}

-- Convert to rows
{% set tests = [] %}
{% for i in range(results.columns[0] | length) %}
    {% set row = [
        results.columns[0][i],
        results.columns[1][i],
        results.columns[2][i],
        results.columns[3][i],
        results.columns[4][i],
        results.columns[5][i]
    ] %}
    {% do tests.append(row) %}
{% endfor %}

{% if tests | length == 0 %}
    {{ log("No active tests found in TEST_CONFIG.", info=True) }}
    {% do return(none) %}
{% endif %}

-- Loop through tests
{% for t in tests %}

    {% set test_id = t[0] %}
    {% set test_name = t[1] %}
    {% set test_type = t[2] %}
    {% set source_table = t[3] %}
    {% set target_table = t[4] %}
    {% set surrogate_key = t[5] %}

    {{ log('Running test: ' ~ test_name ~ ' (' ~ test_type ~ ')', info=True) }}

    {% if test_type == 'row_count' %}

        -- Row Count Test
        {% set sql %}
            WITH src AS (SELECT COUNT(*) AS row_count FROM {{ source_table }}),
                 tgt AS (SELECT COUNT(*) AS row_count FROM {{ target_table }})
            SELECT 
                src.row_count AS source_count,
                tgt.row_count AS target_count,
                (tgt.row_count - src.row_count) AS row_diff
            FROM src, tgt
        {% endset %}

        {% set result = run_query(sql) %}
        {% set row = result.columns[0] %}
        {% set source_count = row[0] %}
        {% set target_count = row[1] %}
        {% set row_diff = row[2] %}
        {% set status = 'PASS' if source_count == target_count else 'FAIL' %}
        {% set details = 'Row count difference: ' ~ row_diff %}

        {% do run_query(
            "INSERT INTO AIRBNB.DEV.TEST_RESULTS 
            (TEST_RUN_ID, TEST_ID, TEST_NAME, TEST_RESULT, RESULT_DETAILS, TIMESTAMP, SOURCE_ROW_COUNT, TARGET_ROW_COUNT, ROW_DIFFERENCE)
            VALUES 
            ('" ~ test_run_id ~ "', " ~ test_id ~ ", '" ~ test_name ~ "', '" ~ status ~ "', '" ~ details ~ "', CURRENT_TIMESTAMP(), NULL, NULL, NULL)"
        ) %}


    {% elif test_type == 'data_match' %}

        -- Data Match Test
        {% set sql %}
            WITH src_hash AS (
                SELECT 
                    {{ surrogate_key }} AS sk,
                    MD5(TO_VARIANT(OBJECT_CONSTRUCT(*))) AS row_hash
                FROM {{ source_table }}
            ),
            tgt_hash AS (
                SELECT 
                    {{ surrogate_key }} AS sk,
                    MD5(TO_VARIANT(OBJECT_CONSTRUCT(*))) AS row_hash
                FROM {{ target_table }}
            ),
            comparison AS (
                SELECT 
                    COALESCE(s.sk, t.sk) AS sk,
                    s.row_hash AS src_hash,
                    t.row_hash AS tgt_hash,
                    CASE 
                        WHEN s.sk IS NULL THEN 'Missing in Source'
                        WHEN t.sk IS NULL THEN 'Missing in Target'
                        WHEN s.row_hash != t.row_hash THEN 'Mismatch'
                        ELSE 'Match'
                    END AS result
                FROM src_hash s
                FULL OUTER JOIN tgt_hash t ON s.sk = t.sk
            )
            SELECT COUNT(*) 
            FROM comparison
            WHERE result != 'Match'
        {% endset %}

        {% set mismatch_count = run_query(sql).columns[0][0] %}
        {% set status = 'PASS' if mismatch_count == 0 else 'FAIL' %}
        {% set details = mismatch_count ~ ' mismatched rows' %}

        {% do run_query(
            "INSERT INTO AIRBNB.DEV.TEST_RESULTS 
            (TEST_RUN_ID, TEST_ID, TEST_NAME, TEST_RESULT, RESULT_DETAILS, TIMESTAMP)
            VALUES 
            ('" ~ test_run_id ~ "', " ~ test_id ~ ", '" ~ test_name ~ "', '" ~ status ~ "', '" ~ details ~ "', CURRENT_TIMESTAMP())"
        ) %}

    {% else %}

        {{ log('Unsupported test type: ' ~ test_type, info=True) }}

    {% endif %}

{% endfor %}

{{ log('Test run complete. Run ID: ' ~ test_run_id, info=True) }}

{% endmacro %}
