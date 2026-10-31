-- Every euro credited somewhere was debited elsewhere, so all balances net to zero.
select sum(signed_amount) as net_total
from {{ ref('stg_ledger_entry') }}
having sum(signed_amount) <> 0
