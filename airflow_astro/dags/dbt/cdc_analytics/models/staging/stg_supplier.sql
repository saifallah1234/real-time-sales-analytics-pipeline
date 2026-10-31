with source as (
    {{ current_state('supplier', 'supplier_id') }}
),

cleaned as (
    select
        src.payload.supplier_id::integer                       as supplier_id,
        trim(src.payload.name::varchar)                        as name,
        trim(src.payload.contact_name::varchar)                as contact_name,
        lower(trim(src.payload.contact_email::varchar))        as contact_email,
        trim(src.payload.contact_phone::varchar)               as contact_phone,
        {{ epoch_ms_to_timestamp('src.payload.created_at') }}  as created_at

    from source as src
)

select *
from cleaned
