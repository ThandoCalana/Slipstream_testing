{% macro entity_metadata() %}

    {% set em_query %}
       " CREATE OR REPLACE TABLE AIRBNB.TESTING.ENTITY_METADATA
        (
            entity_id INT UNIQUE,
            layer_type VARCHAR(100),
            db_name VARCHAR(400),
            table_type VARCHAR(400),
            table_name VARCHAR(400),
            table_schema VARCHAR(400),
            hash_col_name VARCHAR(400),
            PKey_col_name VARCHAR(400),
            NKey_col_name VARCHAR(400),
            SKey_col_name VARCHAR(400),
            effective_date_col_name VARCHAR(400),
            expiration_date_col_name VARCHAR(400),
            process_time_col_name VARCHAR(400),
            is_active_col_name VARCHAR(400)
        ); "

    {% endset %}

    {% return(run_query(em_query)) %}

{% endmacro %}