with source as (

    select *
    from {{ source('postgres_raw', 'inventory_movement') }}
),

cleaned as (
    select
        cast(movement_id as integer)             as movement_id,
        cast(product_id as integer)              as product_id,
        upper(trim(movement_type))               as movement_type,
        cast(quantity as integer)                as quantity,
        cast(movement_date as timestamp_ntz)     as movement_date,
        cast(created_at as timestamp_ntz)        as created_at

    from source
)

select *
from cleaned
