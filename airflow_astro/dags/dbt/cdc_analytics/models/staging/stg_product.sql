with source as (

    select *
    from {{ source('postgres_raw', 'product') }}
),

cleaned as (
    select
        cast(product_id as integer)              as product_id,
        trim(name)                              as name,
        trim(description)                       as description,
        cast(unit_price as number(10, 2))       as unit_price,
        cast(created_at as timestamp_ntz)       as created_at

    from source
)

select *
from cleaned
