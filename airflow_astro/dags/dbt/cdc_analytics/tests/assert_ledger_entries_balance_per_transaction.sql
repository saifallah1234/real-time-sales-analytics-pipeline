-- Double entry: a Posted/Reversed transaction has exactly one Debit and one Credit of its amount;
-- Pending/Failed transactions have no ledger entries. Returns the offending transactions.
with entries as (

    select
        transaction_id,
        count(*)                                                 as entry_count,
        sum(case when direction = 'Debit' then 1 else 0 end)     as debit_count,
        sum(case when direction = 'Debit' then amount else 0 end)  as debit_total,
        sum(case when direction = 'Credit' then amount else 0 end) as credit_total
    from {{ ref('stg_ledger_entry') }}
    group by transaction_id

)

select
    t.transaction_id,
    t.status,
    t.amount,
    e.entry_count,
    e.debit_total,
    e.credit_total

from {{ ref('stg_financial_transaction') }} as t

left join entries as e
    on t.transaction_id = e.transaction_id

where
    (
        t.status in ('Posted', 'Reversed')
        and (
            e.entry_count is null
            or e.entry_count <> 2
            or e.debit_count <> 1
            or e.debit_total <> t.amount
            or e.credit_total <> t.amount
        )
    )
    or (t.status not in ('Posted', 'Reversed') and e.entry_count is not null)
