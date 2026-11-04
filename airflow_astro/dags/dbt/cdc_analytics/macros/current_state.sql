{#
    Current state of one source table, rebuilt from the Debezium event stream:
    the newest event per primary key (by LSN), with keys whose newest event is a delete dropped.
    Returns one SUPER column, `payload`; staging models read typed columns as `src.payload.<column>`.
#}
{% macro current_state(table_name, primary_key) %}
    -- shortcut: re-reads the full event history on every query; switch staging to incremental models if the stream grows to millions of events.
    select latest.payload
    from (
        select e.payload
        from {{ source('cdc_landing', 'cdc_events') }} as e
        where e.payload.__table::varchar = '{{ table_name }}'
        qualify row_number() over (
            partition by e.payload.{{ primary_key }}::bigint
            order by
                e.payload.__lsn::bigint desc,
                e.payload.__source_ts_ms::bigint desc,
                e.approximate_arrival_timestamp desc
        ) = 1
    ) as latest
    where coalesce(latest.payload.__deleted::varchar, 'false') <> 'true'
{% endmacro %}
