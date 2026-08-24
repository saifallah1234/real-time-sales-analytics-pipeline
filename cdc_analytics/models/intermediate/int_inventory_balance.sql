with source as (
    select * from {{ ref('stg_inventory_movement') }} 
),
convert as (
    select
        cast(product_id as integer)              as product_id,
        upper(trim(movement_type))               as movement_type,
        case 
        when movement_type = 'IN' then quantity
        when movement_type = 'OUT' then -quantity
        else 0
        end as quantity
    from source
)

select product_id, sum(quantity) as balance_quantity from convert
group by product_id