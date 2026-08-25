with source as (

    select *
    from {{ source('postgres_raw', 'sales_invoice_item') }}
),

cleaned as (
    select
        cast(item_id as integer)                 as item_id,
        cast(invoice_id as integer)              as invoice_id,
        cast(product_id as integer)              as product_id,
        cast(quantity as integer)                as quantity,
        cast(unit_price as number(10, 2))        as unit_price,
        cast(discount as number(10, 2))          as discount,
        {{discounted_price('unit_price * quantity', 'discount')}} as total_price_with_discount,
        cast(created_at as timestamp_ntz)        as created_at

    from source
)

select *
from cleaned
