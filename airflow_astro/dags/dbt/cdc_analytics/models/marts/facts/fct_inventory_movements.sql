select
    movement_id,
    product_id,

    {{ date_key('movement_date') }} as movement_date_key,

    movement_type,
    quantity,

    case
        when movement_type = 'IN' then quantity
        when movement_type = 'OUT' then -quantity
        else 0
    end as signed_quantity

from {{ ref('stg_inventory_movement') }}
