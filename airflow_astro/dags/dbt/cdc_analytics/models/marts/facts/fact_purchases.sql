select
    li.item_id,
    li.bill_id,
    li.product_id,
    b.supplier_id,

    {{ date_key('b.bill_date') }} as bill_date_key,

    li.quantity,
    li.unit_price,
    li.quantity * li.unit_price as gross_amount,
    b.paid_amount,
    b.status as bill_status

from {{ ref('stg_purchase_bill_item') }} as li

inner join {{ ref('stg_purchase_bill') }} as b
    on li.bill_id = b.bill_id
