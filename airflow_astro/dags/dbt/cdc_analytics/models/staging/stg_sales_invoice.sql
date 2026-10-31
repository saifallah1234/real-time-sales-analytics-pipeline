with source as (
    {{ current_state('sales_invoice', 'invoice_id') }}
),

cleaned as (
    select
        src.payload.invoice_id::integer                         as invoice_id,
        src.payload.customer_id::integer                        as customer_id,
        {{ epoch_ms_to_timestamp('src.payload.invoice_date') }} as invoice_date,
        src.payload.total_sum::varchar::decimal(10, 2)          as total_sum,
        src.payload.tax_amount::varchar::decimal(10, 2)         as tax_amount,
        {{ tax_price('total_sum', 'tax_amount') }}              as total_amount_with_tax,
        trim(src.payload.status::varchar)                       as status,
        {{ epoch_ms_to_timestamp('src.payload.created_at') }}   as created_at

    from source as src
)

select *
from cleaned
