with payments as (

    select
        bill_id,
        sum(amount) as ledger_paid_amount
    from {{ ref('stg_financial_transaction') }}
    where transaction_type = 'Bill Payment'
      and status = 'Posted'
    group by bill_id

),

payables as (

    select
        b.bill_id,
        b.supplier_id,
        {{ date_key('b.bill_date') }}        as bill_date_key,
        b.status                             as bill_status,
        b.total_amount_with_tax              as amount_due,
        b.paid_amount                        as recorded_paid_amount,
        coalesce(p.ledger_paid_amount, 0)    as ledger_paid_amount
    from {{ ref('stg_purchase_bill') }} as b
    left join payments as p
        on b.bill_id = p.bill_id

)

select
    *,
    amount_due - ledger_paid_amount as outstanding_amount,
    -- The bill's own paid_amount must match what actually left the operating account.
    coalesce(recorded_paid_amount, 0) <> ledger_paid_amount as is_paid_amount_mismatch

from payables
