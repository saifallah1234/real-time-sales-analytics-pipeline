select
    entry_id,
    transaction_id,
    account_id,
    {{ date_key('created_at') }} as entry_date_key,
    direction,
    amount,
    signed_amount

from {{ ref('stg_ledger_entry') }}
