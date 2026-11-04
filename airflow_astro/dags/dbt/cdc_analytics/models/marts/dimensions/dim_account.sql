select
    a.account_id,
    a.account_number,
    a.customer_id,
    a.supplier_id,
    a.account_type,
    a.currency,
    a.status,
    a.opened_at,
    a.created_at,
    -- Internal accounts can be negative: Clearing is where money enters from outside.
    coalesce(b.balance, 0) as current_balance

from {{ ref('stg_account') }} as a

left join {{ ref('int_account_balance') }} as b
    on a.account_id = b.account_id
