with source as (

    select *
    from {{ source('postgres_raw', 'purchase_bill') }}
),

cleaned as (
    select
        cast(bill_id as integer)                 as bill_id,
        cast(supplier_id as integer)             as supplier_id,
        cast(bill_date as timestamp_ntz)         as bill_date,
        cast(total_sum as number(10, 2))         as total_sum,
        cast(tax_amount as number(10, 2))        as tax_amount,
        {{tax_price('total_sum','tax_amount')}} as total_amount_with_tax,
        cast(paid_amount as number(10, 2))       as paid_amount,
        cast(total_amount_with_tax - paid_amount as number(10, 2)) as outstanding_amount,
        trim(status)                            as status,
        cast(created_at as timestamp_ntz)       as created_at

    from source
)

select *
from cleaned
