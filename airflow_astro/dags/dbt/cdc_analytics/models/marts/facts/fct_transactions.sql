with legs as (

    -- Only Posted/Reversed transactions have ledger entries; others get null accounts.
    select
        transaction_id,
        max(case when direction = 'Debit' then account_id end)  as from_account_id,
        max(case when direction = 'Credit' then account_id end) as to_account_id
    from {{ ref('stg_ledger_entry') }}
    group by transaction_id

)

select
    t.transaction_id,
    t.transaction_type,
    t.status,
    t.amount,
    t.currency,
    {{ date_key('t.transaction_date') }} as transaction_date_key,
    t.transaction_date,
    t.card_id,
    t.invoice_id,
    t.bill_id,
    t.reverses_transaction_id,
    l.from_account_id,
    l.to_account_id,
    t.status = 'Reversed' as is_reversed,
    t.status in ('Posted', 'Reversed') and t.transaction_type <> 'Reversal' as is_successful

from {{ ref('stg_financial_transaction') }} as t

left join legs as l
    on t.transaction_id = l.transaction_id
