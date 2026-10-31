with source as (
    {{ current_state('inventory_movement', 'movement_id') }}
),

cleaned as (
    select
        src.payload.movement_id::integer                          as movement_id,
        src.payload.product_id::integer                           as product_id,
        upper(trim(src.payload.movement_type::varchar))           as movement_type,
        src.payload.quantity::integer                             as quantity,
        {{ epoch_ms_to_timestamp('src.payload.movement_date') }}  as movement_date,
        {{ epoch_ms_to_timestamp('src.payload.created_at') }}     as created_at

    from source as src
)

select *
from cleaned
