with source as (

    select *
    from {{ source('postgres_raw', 'purchase_bill_item') }}
),

cleaned as (
    select
        cast(item_id as integer)                 as item_id,
        cast(bill_id as integer)                 as bill_id,
        cast(product_id as integer)              as product_id,
        cast(quantity as integer)                as quantity,
        cast(unit_price as number(10, 2))        as unit_price,
        cast(created_at as timestamp_ntz)        as created_at

    from source
)

select *
from cleaned
