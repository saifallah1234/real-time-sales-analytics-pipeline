select 
    p.product_id,
    p.name,
    p.description,
    p.unit_price,
    inv.balance_quantity,
from 
    {{ref('stg_product')}} as p
left join 
    {{ref('int_inventory_balance')}} as inv
on 
    p.product_id = inv.product_id