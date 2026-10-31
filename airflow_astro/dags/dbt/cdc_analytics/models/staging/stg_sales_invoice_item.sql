with source as (
    {{ current_state('sales_invoice_item', 'item_id') }}
),

cleaned as (
    select
        src.payload.item_id::integer                           as item_id,
        src.payload.invoice_id::integer                        as invoice_id,
        src.payload.product_id::integer                        as product_id,
        src.payload.quantity::integer                          as quantity,
        src.payload.unit_price::varchar::decimal(10, 2)        as unit_price,
        src.payload.discount::varchar::decimal(10, 2)          as discount,
        {{ discounted_price('unit_price * quantity', 'discount') }} as total_price_with_discount,
        {{ epoch_ms_to_timestamp('src.payload.created_at') }}  as created_at

    from source as src
)

select *
from cleaned
