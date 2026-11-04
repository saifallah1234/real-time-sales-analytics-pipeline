with source as (
    {{ current_state('card', 'card_id') }}
),

cleaned as (
    select
        src.payload.card_id::integer                           as card_id,
        src.payload.account_id::integer                        as account_id,
        trim(src.payload.card_type::varchar)                   as card_type,
        trim(src.payload.brand::varchar)                       as brand,
        -- last4 only: the source never stores full card numbers (PCI DSS).
        src.payload.last4::varchar                             as last4,
        src.payload.expiry_month::smallint                     as expiry_month,
        src.payload.expiry_year::smallint                      as expiry_year,
        trim(src.payload.status::varchar)                      as status,
        {{ epoch_ms_to_timestamp('src.payload.created_at') }}  as created_at

    from source as src
)

select *
from cleaned
