with source as (
    {{ current_state('purchase_bill', 'bill_id') }}
),

cleaned as (
    select
        src.payload.bill_id::integer                           as bill_id,
        src.payload.supplier_id::integer                       as supplier_id,
        {{ epoch_ms_to_timestamp('src.payload.bill_date') }}   as bill_date,
        src.payload.total_sum::varchar::decimal(10, 2)         as total_sum,
        src.payload.tax_amount::varchar::decimal(10, 2)        as tax_amount,
        {{ tax_price('total_sum', 'tax_amount') }}             as total_amount_with_tax,
        src.payload.paid_amount::varchar::decimal(10, 2)       as paid_amount,
        cast(total_amount_with_tax - paid_amount as decimal(10, 2)) as outstanding_amount,
        trim(src.payload.status::varchar)                      as status,
        {{ epoch_ms_to_timestamp('src.payload.created_at') }}  as created_at

    from source as src
)

select *
from cleaned
