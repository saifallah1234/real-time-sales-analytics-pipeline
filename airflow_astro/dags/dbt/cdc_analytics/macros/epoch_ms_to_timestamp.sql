{# Debezium time.precision.mode=connect sends TIMESTAMP columns as epoch milliseconds. #}
{% macro epoch_ms_to_timestamp(column) %}
    (timestamp 'epoch' + {{ column }}::bigint / 1000.0 * interval '1 second')
{% endmacro %}
