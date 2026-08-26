select
    movement_id,
    product_id,

    to_number(
        to_char(cast(movement_date as date), 'YYYYMMDD')
    ) as movement_date_key,

    movement_type,
    quantity,

    case
        when movement_type = 'IN' then quantity
        when movement_type = 'OUT' then -quantity
        else 0
    end as signed_quantity

from {{ ref('stg_inventory_movement') }}
