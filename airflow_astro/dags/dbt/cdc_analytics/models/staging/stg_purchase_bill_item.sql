with source as (
    {{ current_state('purchase_bill_item', 'item_id') }}
),

cleaned as (
    select
        src.payload.item_id::integer                           as item_id,
        src.payload.bill_id::integer                           as bill_id,
        src.payload.product_id::integer                        as product_id,
        src.payload.quantity::integer                          as quantity,
        src.payload.unit_price::varchar::decimal(10, 2)        as unit_price,
        {{ epoch_ms_to_timestamp('src.payload.created_at') }}  as created_at

    from source as src
)

select *
from cleaned
