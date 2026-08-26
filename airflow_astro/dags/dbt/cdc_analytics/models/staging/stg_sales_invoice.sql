with source as (

    select *
    from {{ source('postgres_raw', 'sales_invoice') }}
),

cleaned as (
    select
        cast(invoice_id as integer)              as invoice_id,
        cast(customer_id as integer)             as customer_id,
        cast(invoice_date as timestamp_ntz)      as invoice_date,
        cast(total_sum as number(10, 2))         as total_sum,
        cast(tax_amount as number(10, 2))        as tax_amount,
        {{tax_price('total_sum','tax_amount')}} as total_amount_with_tax,
        trim(status)                            as status,
        cast(created_at as timestamp_ntz)       as created_at

    from source
)

select *
from cleaned
