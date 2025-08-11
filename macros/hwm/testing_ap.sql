{% macro testing_ap(ap) %}
    {{log("We are here", info=True)}}
    {{ log(ap, info=True)}}

    {% set query %}
        SELECT current_process_ts
        FROM AIRBNB.TESTING.HIGH_WATERMARK
        WHERE table_name = {{ ap }}
    {% endset %}
    {{ log("Query: " ~ query, info=True)}}
    

    
{% endmacro %}
