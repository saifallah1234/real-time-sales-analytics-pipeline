with source as (

    select *
    from {{ source('postgres_raw', 'customer') }}),

cleaned as (
    select
        customer_id,
        trim(first_name)                         as first_name,
        trim(last_name)                          as last_name,
        lower(trim(email))                       as email,
        trim(phone_number)                       as phone_number,
        cast(created_at as timestamp_ntz)        as created_at

    from source)

select *
from cleaned