with source as (
    {{ current_state('financial_transaction', 'transaction_id') }}
),

cleaned as (
    select
        src.payload.transaction_id::integer                         as transaction_id,
        trim(src.payload.transaction_type::varchar)                 as transaction_type,
        trim(src.payload.status::varchar)                           as status,
        src.payload.amount::varchar::decimal(12, 2)                 as amount,
        upper(trim(src.payload.currency::varchar))                  as currency,
        src.payload.card_id::integer                                as card_id,
        src.payload.invoice_id::integer                             as invoice_id,
        src.payload.bill_id::integer                                as bill_id,
        src.payload.reverses_transaction_id::integer                as reverses_transaction_id,
        trim(src.payload.description::varchar)                      as description,
        {{ epoch_ms_to_timestamp('src.payload.transaction_date') }} as transaction_date,
        {{ epoch_ms_to_timestamp('src.payload.created_at') }}       as created_at

    from source as src
)

select *
from cleaned
