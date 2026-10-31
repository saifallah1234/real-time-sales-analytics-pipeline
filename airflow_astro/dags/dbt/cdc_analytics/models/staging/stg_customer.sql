with source as (
    {{ current_state('customer', 'customer_id') }}
),

cleaned as (
    select
        src.payload.customer_id::integer                       as customer_id,
        trim(src.payload.first_name::varchar)                  as first_name,
        trim(src.payload.last_name::varchar)                   as last_name,
        lower(trim(src.payload.email::varchar))                as email,
        trim(src.payload.phone_number::varchar)                as phone_number,
        {{ epoch_ms_to_timestamp('src.payload.created_at') }}  as created_at

    from source as src
)

select *
from cleaned
