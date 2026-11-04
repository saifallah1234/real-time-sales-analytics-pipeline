with payments as (

    -- Reversed payments drop out: their status is no longer Posted.
    select
        invoice_id,
        sum(amount) as amount_paid
    from {{ ref('stg_financial_transaction') }}
    where transaction_type = 'Invoice Payment'
      and status = 'Posted'
    group by invoice_id

),

receivables as (

    select
        i.invoice_id,
        i.customer_id,
        {{ date_key('i.invoice_date') }}   as invoice_date_key,
        i.status                           as invoice_status,
        i.total_amount_with_tax            as amount_due,
        coalesce(p.amount_paid, 0)         as amount_paid
    from {{ ref('stg_sales_invoice') }} as i
    left join payments as p
        on i.invoice_id = p.invoice_id

)

select
    *,
    amount_due - amount_paid as outstanding_amount,
    -- Status must say Paid exactly when the ledger shows the invoice fully paid.
    case
        when invoice_status = 'Paid' and amount_paid < amount_due then true
        when invoice_status <> 'Paid' and amount_paid >= amount_due then true
        else false
    end as is_status_mismatch

from receivables
