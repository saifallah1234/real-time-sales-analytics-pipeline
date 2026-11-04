with source as (
    {{ current_state('product', 'product_id') }}
),

cleaned as (
    select
        src.payload.product_id::integer                        as product_id,
        trim(src.payload.name::varchar)                        as name,
        trim(src.payload.description::varchar)                 as description,
        src.payload.unit_price::varchar::decimal(10, 2)        as unit_price,
        {{ epoch_ms_to_timestamp('src.payload.created_at') }}  as created_at

    from source as src
)

select *
from cleaned
