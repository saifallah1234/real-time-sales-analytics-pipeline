with source as (
    {{ current_state('account', 'account_id') }}
),

cleaned as (
    select
        src.payload.account_id::integer                        as account_id,
        trim(src.payload.account_number::varchar)              as account_number,
        src.payload.customer_id::integer                       as customer_id,
        src.payload.supplier_id::integer                       as supplier_id,
        trim(src.payload.account_type::varchar)                as account_type,
        upper(trim(src.payload.currency::varchar))             as currency,
        trim(src.payload.status::varchar)                      as status,
        {{ epoch_ms_to_timestamp('src.payload.opened_at') }}   as opened_at,
        {{ epoch_ms_to_timestamp('src.payload.created_at') }}  as created_at

    from source as src
)

select *
from cleaned
