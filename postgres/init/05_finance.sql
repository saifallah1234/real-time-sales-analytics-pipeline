-- Fintech-style tables: accounts, cards, transactions and a double-entry ledger.
-- Runs after 03_add_updated_at.sql (reuses its set_updated_at() trigger function).

\connect cdc_project

SET search_path TO cdc_schema;

BEGIN;

CREATE TABLE IF NOT EXISTS account (
    account_id SERIAL PRIMARY KEY,
    account_number VARCHAR(20) UNIQUE NOT NULL,
    customer_id INT REFERENCES customer(customer_id),
    supplier_id INT REFERENCES supplier(supplier_id),
    account_type VARCHAR(20) NOT NULL
        CHECK (account_type IN ('Checking', 'Savings', 'Business', 'Operating', 'Clearing')),
    currency CHAR(3) NOT NULL DEFAULT 'EUR',
    status VARCHAR(20) NOT NULL DEFAULT 'Active' CHECK (status IN ('Active', 'Frozen', 'Closed')),
    opened_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT account_single_owner_check CHECK (customer_id IS NULL OR supplier_id IS NULL),
    CONSTRAINT account_internal_type_check CHECK (
        (customer_id IS NULL AND supplier_id IS NULL) = (account_type IN ('Operating', 'Clearing'))
    )
);
COMMENT ON COLUMN account.account_type IS 'Checking/Savings for customers, Business for suppliers, Operating/Clearing for the company.';

CREATE TABLE IF NOT EXISTS card (
    card_id SERIAL PRIMARY KEY,
    account_id INT NOT NULL REFERENCES account(account_id),
    card_type VARCHAR(20) NOT NULL CHECK (card_type IN ('Credit', 'Debit')),
    brand VARCHAR(20) NOT NULL CHECK (brand IN ('Visa', 'Mastercard')),
    -- Never store the full card number (PAN) in plain text (PCI DSS); keep the last 4 digits only.
    last4 CHAR(4) NOT NULL CHECK (last4 ~ '^[0-9]{4}$'),
    expiry_month SMALLINT NOT NULL CHECK (expiry_month BETWEEN 1 AND 12),
    expiry_year SMALLINT NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'Active' CHECK (status IN ('Active', 'Blocked', 'Expired')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

-- Named financial_transaction because TRANSACTION is a keyword in Postgres, Redshift and Snowflake.
CREATE TABLE IF NOT EXISTS financial_transaction (
    transaction_id SERIAL PRIMARY KEY,
    transaction_type VARCHAR(20) NOT NULL CHECK (transaction_type IN (
        'Deposit', 'Withdrawal', 'Transfer', 'Card Payment',
        'Invoice Payment', 'Bill Payment', 'Reversal'
    )),
    status VARCHAR(20) NOT NULL DEFAULT 'Pending'
        CHECK (status IN ('Pending', 'Posted', 'Failed', 'Reversed')),
    amount DECIMAL(12, 2) NOT NULL CHECK (amount > 0),
    currency CHAR(3) NOT NULL DEFAULT 'EUR',
    card_id INT REFERENCES card(card_id),
    invoice_id INT REFERENCES sales_invoice(invoice_id),
    bill_id INT REFERENCES purchase_bill(bill_id),
    reverses_transaction_id INT REFERENCES financial_transaction(transaction_id),
    description VARCHAR(200),
    transaction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT transaction_invoice_check CHECK ((transaction_type = 'Invoice Payment') = (invoice_id IS NOT NULL)),
    CONSTRAINT transaction_bill_check CHECK ((transaction_type = 'Bill Payment') = (bill_id IS NOT NULL)),
    CONSTRAINT transaction_reversal_check CHECK ((transaction_type = 'Reversal') = (reverses_transaction_id IS NOT NULL)),
    CONSTRAINT transaction_card_check CHECK (transaction_type <> 'Card Payment' OR card_id IS NOT NULL)
);
COMMENT ON COLUMN financial_transaction.status IS 'Pending -> Posted | Failed; Posted -> Reversed (via a new Reversal transaction). Only Posted transactions have ledger entries.';

-- Append-only double-entry ledger: each Posted transaction has a Debit (money out of an account)
-- and a Credit (money into an account) of equal amount. Balance = credits - debits.
-- shortcut: debits = credits per transaction is checked by the seed self-check and dbt tests, not by a DB trigger.
CREATE TABLE IF NOT EXISTS ledger_entry (
    entry_id SERIAL PRIMARY KEY,
    transaction_id INT NOT NULL REFERENCES financial_transaction(transaction_id),
    account_id INT NOT NULL REFERENCES account(account_id),
    direction VARCHAR(6) NOT NULL CHECK (direction IN ('Debit', 'Credit')),
    amount DECIMAL(12, 2) NOT NULL CHECK (amount > 0),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

DROP TRIGGER IF EXISTS set_account_updated_at ON account;
CREATE TRIGGER set_account_updated_at
BEFORE UPDATE ON account
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS set_card_updated_at ON card;
CREATE TRIGGER set_card_updated_at
BEFORE UPDATE ON card
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS set_financial_transaction_updated_at ON financial_transaction;
CREATE TRIGGER set_financial_transaction_updated_at
BEFORE UPDATE ON financial_transaction
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS set_ledger_entry_updated_at ON ledger_entry;
CREATE TRIGGER set_ledger_entry_updated_at
BEFORE UPDATE ON ledger_entry
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- 01_init.sql's grants only covered tables that existed at that time.
GRANT SELECT, INSERT, UPDATE, DELETE
ON account, card, financial_transaction, ledger_entry
TO cdc_administrator;

GRANT USAGE, SELECT
ON ALL SEQUENCES IN SCHEMA cdc_schema
TO cdc_administrator;

COMMIT;
