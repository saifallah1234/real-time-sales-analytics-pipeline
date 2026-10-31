{# The streaming MV is AUTO REFRESH NO (cost): pull new Kinesis events. Run once before dbt: `dbt run-operation refresh_landing`. #}
{% macro refresh_landing() %}
    {% do run_query("REFRESH MATERIALIZED VIEW " ~ source('cdc_landing', 'cdc_events')) %}
{% endmacro %}
