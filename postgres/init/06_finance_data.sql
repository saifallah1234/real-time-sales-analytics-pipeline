-- Synthetic fintech data for the 05_finance.sql tables.
-- Runs after 02_synthetic_data.sql: payments are derived from the final invoice/bill state there.
-- Assumes the finance tables are empty.

\connect cdc_project

SET search_path TO cdc_schema;

BEGIN;

-- ---------------------------------------------------------------------------
-- Accounts: 1 = company operating, 2 = external clearing,
-- 3-12 = customer checking (customer 1-10), 13-22 = supplier business (supplier 1-10).
-- ---------------------------------------------------------------------------

INSERT INTO account
    (account_id, account_number, customer_id, supplier_id, account_type, opened_at, created_at)
VALUES
    (1, 'ACC-00000001', NULL, NULL, 'Operating', TIMESTAMP '2025-08-01 08:00:00', TIMESTAMP '2025-08-01 08:00:00'),
    (2, 'ACC-00000002', NULL, NULL, 'Clearing',  TIMESTAMP '2025-08-01 08:00:00', TIMESTAMP '2025-08-01 08:00:00');

INSERT INTO account
    (account_id, account_number, customer_id, supplier_id, account_type, opened_at, created_at)
SELECT
    customer_id + 2,
    'ACC-' || LPAD((customer_id + 2)::TEXT, 8, '0'),
    customer_id, NULL, 'Checking', created_at, created_at
FROM customer
WHERE customer_id BETWEEN 1 AND 10
ORDER BY customer_id;

INSERT INTO account
    (account_id, account_number, customer_id, supplier_id, account_type, opened_at, created_at)
SELECT
    supplier_id + 12,
    'ACC-' || LPAD((supplier_id + 12)::TEXT, 8, '0'),
    NULL, supplier_id, 'Business', created_at, created_at
FROM supplier
WHERE supplier_id BETWEEN 1 AND 10
ORDER BY supplier_id;

-- ---------------------------------------------------------------------------
-- Cards: one debit card per customer (card_id = customer_id), plus credit cards
-- 11 and 12 for customers 1 and 2.
-- ---------------------------------------------------------------------------

INSERT INTO card
    (card_id, account_id, card_type, brand, last4, expiry_month, expiry_year, created_at)
SELECT
    customer_id,
    customer_id + 2,
    'Debit',
    CASE WHEN customer_id % 2 = 0 THEN 'Mastercard' ELSE 'Visa' END,
    LPAD(((customer_id * 1373) % 10000)::TEXT, 4, '0'),
    customer_id,
    2028,
    created_at + INTERVAL '1 day'
FROM customer
WHERE customer_id BETWEEN 1 AND 10
ORDER BY customer_id;

INSERT INTO card
    (card_id, account_id, card_type, brand, last4, expiry_month, expiry_year, created_at)
VALUES
    (11, 3, 'Credit', 'Visa',       '4821', 6, 2029, TIMESTAMP '2025-09-15 10:00:00'),
    (12, 4, 'Credit', 'Mastercard', '7310', 9, 2029, TIMESTAMP '2025-09-16 10:00:00');

-- ---------------------------------------------------------------------------
-- Transactions. Each seed row names the account money leaves (from_account)
-- and the account it enters (to_account); Posted rows become two ledger entries.
-- ID ranges: 1-11 deposits, 100+invoice_id, 200+bill_id, 300+ miscellaneous.
-- ---------------------------------------------------------------------------

CREATE TEMP TABLE seed_txn (
    transaction_id INT,
    transaction_type VARCHAR(20),
    status VARCHAR(20),
    amount DECIMAL(12, 2),
    card_id INT,
    invoice_id INT,
    bill_id INT,
    description VARCHAR(200),
    transaction_date TIMESTAMP,
    from_account INT,
    to_account INT
) ON COMMIT DROP;

-- Opening capital for the company and a starting deposit for every customer.
INSERT INTO seed_txn
VALUES (1, 'Deposit', 'Posted', 250000.00, NULL, NULL, NULL,
        'Opening capital', TIMESTAMP '2025-08-01 09:00:00', 2, 1);

INSERT INTO seed_txn
SELECT customer_id + 1, 'Deposit', 'Posted', 10000.00, NULL, NULL, NULL,
       'Salary deposit', created_at + INTERVAL '1 hour', 2, customer_id + 2
FROM customer
WHERE customer_id BETWEEN 1 AND 10;

-- Every Paid sales invoice is settled in full (total + tax) from the customer's account.
INSERT INTO seed_txn
SELECT 100 + invoice_id, 'Invoice Payment', 'Posted', total_sum + tax_amount, NULL, invoice_id, NULL,
       'Payment of invoice ' || invoice_id, invoice_date + INTERVAL '3 days', customer_id + 2, 1
FROM sales_invoice
WHERE status = 'Paid';

-- Every purchase bill with a paid_amount (Paid or Partially Paid) is paid to the supplier.
INSERT INTO seed_txn
SELECT 200 + bill_id, 'Bill Payment', 'Posted', paid_amount, NULL, NULL, bill_id,
       'Payment of bill ' || bill_id, bill_date + INTERVAL '5 days', 1, supplier_id + 12
FROM purchase_bill
WHERE paid_amount > 0;

INSERT INTO seed_txn VALUES
    (301, 'Card Payment',    'Posted',  59.90, 1,    NULL, NULL, 'Card purchase: Webcam HD',
     TIMESTAMP '2026-08-12 14:20:00', 3, 1),
    (302, 'Transfer',        'Posted', 150.00, NULL, NULL, NULL, 'Shared dinner',
     TIMESTAMP '2026-08-13 20:05:00', 4, 5),
    (303, 'Withdrawal',      'Posted', 200.00, 4,    NULL, NULL, 'ATM withdrawal',
     TIMESTAMP '2026-08-14 09:30:00', 6, 2),
    -- Insufficient-funds style failure on an Overdue invoice: no ledger entries.
    (304, 'Invoice Payment', 'Failed', (SELECT total_sum + tax_amount FROM sales_invoice WHERE invoice_id = 5),
     NULL, 5, NULL, 'Payment of invoice 5 (declined)', TIMESTAMP '2026-08-15 10:00:00', 7, 1),
    -- Posted later in the CDC events section.
    (305, 'Transfer',        'Pending',  75.00, NULL, NULL, NULL, 'Rent share',
     TIMESTAMP '2026-08-16 18:00:00', 7, 8),
    -- Reversed later in the CDC events section.
    (306, 'Card Payment',    'Posted',  89.00, 11,   NULL, NULL, 'Card purchase: Noise-Cancel Headset',
     TIMESTAMP '2026-08-17 11:45:00', 3, 1);

INSERT INTO financial_transaction
    (transaction_id, transaction_type, status, amount, card_id, invoice_id, bill_id,
     description, transaction_date, created_at)
SELECT transaction_id, transaction_type, status, amount, card_id, invoice_id, bill_id,
       description, transaction_date, transaction_date
FROM seed_txn
ORDER BY transaction_id;

INSERT INTO ledger_entry (transaction_id, account_id, direction, amount, created_at)
SELECT s.transaction_id, side.account_id, side.direction, s.amount, s.transaction_date
FROM seed_txn s
CROSS JOIN LATERAL (VALUES
    (s.from_account, 'Debit'),
    (s.to_account, 'Credit')
) AS side(account_id, direction)
WHERE s.status = 'Posted'
ORDER BY s.transaction_id, side.direction DESC;

-- ---------------------------------------------------------------------------
-- CDC exercise events: updates, a reversal, and a delete + replacement insert.
-- ---------------------------------------------------------------------------

-- Pending transfer settles: status UPDATE plus its ledger INSERTs.
UPDATE financial_transaction
SET status = 'Posted'
WHERE transaction_id = 305;

INSERT INTO ledger_entry (transaction_id, account_id, direction, amount, created_at)
VALUES
    (305, 7, 'Debit',  75.00, TIMESTAMP '2026-08-17 09:00:00'),
    (305, 8, 'Credit', 75.00, TIMESTAMP '2026-08-17 09:00:00');

-- Card payment is reversed: the original stays in the ledger, a Reversal undoes it.
UPDATE financial_transaction
SET status = 'Reversed'
WHERE transaction_id = 306;

INSERT INTO financial_transaction
    (transaction_id, transaction_type, status, amount, reverses_transaction_id,
     description, transaction_date, created_at)
VALUES
    (307, 'Reversal', 'Posted', 89.00, 306, 'Refund: Noise-Cancel Headset',
     TIMESTAMP '2026-08-19 15:00:00', TIMESTAMP '2026-08-19 15:00:00');

INSERT INTO ledger_entry (transaction_id, account_id, direction, amount, created_at)
VALUES
    (307, 1, 'Debit',  89.00, TIMESTAMP '2026-08-19 15:00:00'),
    (307, 3, 'Credit', 89.00, TIMESTAMP '2026-08-19 15:00:00');

-- Lost card is blocked; an account is frozen.
UPDATE card SET status = 'Blocked' WHERE card_id = 3;
UPDATE account SET status = 'Frozen' WHERE account_id = 12;

-- Unused credit card is cancelled and re-issued (DELETE + INSERT events).
DELETE FROM card WHERE card_id = 12;

INSERT INTO card
    (card_id, account_id, card_type, brand, last4, expiry_month, expiry_year, created_at)
VALUES
    (13, 4, 'Credit', 'Mastercard', '9054', 10, 2030, TIMESTAMP '2026-08-20 10:00:00');

-- Explicit IDs were used for reproducibility; advance every SERIAL sequence.
SELECT setval(pg_get_serial_sequence('account', 'account_id'),
              (SELECT MAX(account_id) FROM account), true);
SELECT setval(pg_get_serial_sequence('card', 'card_id'),
              (SELECT MAX(card_id) FROM card), true);
SELECT setval(pg_get_serial_sequence('financial_transaction', 'transaction_id'),
              (SELECT MAX(transaction_id) FROM financial_transaction), true);
SELECT setval(pg_get_serial_sequence('ledger_entry', 'entry_id'),
              (SELECT MAX(entry_id) FROM ledger_entry), true);

-- Self-check: the seed must be a valid double-entry ledger.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM ledger_entry
        GROUP BY transaction_id
        HAVING SUM(CASE WHEN direction = 'Debit' THEN amount ELSE 0 END)
            <> SUM(CASE WHEN direction = 'Credit' THEN amount ELSE 0 END)
            OR COUNT(*) < 2
    ) THEN
        RAISE EXCEPTION 'Unbalanced ledger: debits <> credits for at least one transaction';
    END IF;

    IF EXISTS (
        SELECT 1 FROM financial_transaction t
        WHERE (t.status IN ('Posted', 'Reversed')) <> EXISTS (
            SELECT 1 FROM ledger_entry e WHERE e.transaction_id = t.transaction_id
        )
    ) THEN
        RAISE EXCEPTION 'Only Posted/Reversed transactions may have ledger entries';
    END IF;

    IF EXISTS (
        SELECT 1 FROM ledger_entry e
        JOIN account a ON a.account_id = e.account_id
        WHERE a.account_type <> 'Clearing'
        GROUP BY e.account_id
        HAVING SUM(CASE WHEN e.direction = 'Credit' THEN e.amount ELSE -e.amount END) < 0
    ) THEN
        RAISE EXCEPTION 'Negative balance on a non-clearing account';
    END IF;
END $$;

COMMIT;
