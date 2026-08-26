select
    li.item_id,
    li.invoice_id,
    li.product_id,
    i.customer_id,

    to_number(
        to_char(cast(i.invoice_date as date), 'YYYYMMDD')
    ) as invoice_date_key,

    li.quantity,
    li.unit_price,
    li.discount as discount_percentage,

    li.quantity * li.unit_price as gross_amount,

    (li.quantity * li.unit_price)
        - li.total_price_with_discount as discount_amount,

    li.total_price_with_discount as net_amount,

    i.status as invoice_status

from {{ ref('stg_sales_invoice_item') }} as li

inner join {{ ref('stg_sales_invoice') }} as i
    on li.invoice_id = i.invoice_id

