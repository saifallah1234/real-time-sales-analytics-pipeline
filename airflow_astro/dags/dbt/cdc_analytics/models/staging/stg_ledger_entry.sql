with source as (
    {{ current_state('ledger_entry', 'entry_id') }}
),

cleaned as (
    select
        src.payload.entry_id::integer                          as entry_id,
        src.payload.transaction_id::integer                    as transaction_id,
        src.payload.account_id::integer                        as account_id,
        trim(src.payload.direction::varchar)                   as direction,
        src.payload.amount::varchar::decimal(12, 2)            as amount,
        -- Balance = credits - debits, so a Credit adds money to the account.
        case when direction = 'Credit' then amount else -amount end as signed_amount,
        {{ epoch_ms_to_timestamp('src.payload.created_at') }}  as created_at

    from source as src
)

select *
from cleaned
