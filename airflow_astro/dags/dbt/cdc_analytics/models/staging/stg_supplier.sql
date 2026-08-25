with source as (

    select *
    from {{ source('postgres_raw', 'supplier') }}
),

cleaned as (
    select
        cast(supplier_id as integer)             as supplier_id,
        trim(name)                              as name,
        trim(contact_name)                      as contact_name,
        lower(trim(contact_email))              as contact_email,
        trim(contact_phone)                     as contact_phone,
        cast(created_at as timestamp_ntz)       as created_at

    from source
)

select *
from cleaned
