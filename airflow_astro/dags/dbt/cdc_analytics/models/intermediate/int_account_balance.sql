select
    account_id,
    sum(signed_amount) as balance
from {{ ref('stg_ledger_entry') }}
group by account_id
