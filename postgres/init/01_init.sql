create user saif with login password 'saif';

create role cdc_administrator with NOLOGIN;

GRANT cdc_administrator TO saif;

create database cdc_project;

\connect cdc_project

create schema cdc_schema;

SET search_path TO cdc_schema;

GRANT CONNECT ON DATABASE cdc_project
TO cdc_administrator;

GRANT USAGE, CREATE ON SCHEMA cdc_schema
TO cdc_administrator;

ALTER ROLE saif
IN DATABASE cdc_project
SET search_path TO cdc_schema;

CREATE TABLE IF NOT EXISTS customer (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    phone_number VARCHAR(20) UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS supplier  (
    supplier_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    contact_name VARCHAR(100),
    contact_email VARCHAR(100) UNIQUE,
    contact_phone VARCHAR(20) UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS product (
    product_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    description TEXT,
    unit_price DECIMAL(10, 2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS sales_invoice (
    invoice_id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES customer(customer_id),
    invoice_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_sum DECIMAL(10, 2) NOT NULL,
    tax_amount DECIMAL(10, 2) NOT NULL,
    status VARCHAR(20) DEFAULT 'Pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON COLUMN sales_invoice.total_sum IS 'The total sum of the invoice before tax.';
COMMENT ON COLUMN sales_invoice.tax_amount IS 'The amount of tax applied to the invoice.';
COMMENT ON COLUMN sales_invoice.status IS 'The current status of the invoice, e.g., Pending, Paid, Cancelled.';

CREATE TABLE IF NOT EXISTS sales_invoice_item (
    item_id SERIAL PRIMARY KEY,
    invoice_id INT REFERENCES sales_invoice(invoice_id),
    product_id INT REFERENCES product(product_id),
    quantity INT NOT NULL CHECK (quantity > 0),
    unit_price DECIMAL(10, 2) NOT NULL CHECK (unit_price >= 0),
    discount DECIMAL(10,2) DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS purchase_bill (
    bill_id SERIAL PRIMARY KEY,
    supplier_id INT REFERENCES supplier(supplier_id),
    bill_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_sum DECIMAL(10, 2) NOT NULL,
    tax_amount DECIMAL(10, 2) NOT NULL,
    paid_amount DECIMAL(10, 2) DEFAULT 0,
    status VARCHAR(20) DEFAULT 'Pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON COLUMN purchase_bill.paid_amount IS 'The amount that has been paid';
CREATE TABLE IF NOT EXISTS purchase_bill_item (
    item_id SERIAL PRIMARY KEY,
    bill_id INT REFERENCES purchase_bill(bill_id),
    product_id INT REFERENCES product(product_id),
    quantity INT NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL CHECK (unit_price >= 0),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS inventory_movement (
    movement_id SERIAL PRIMARY KEY,
    product_id INT REFERENCES product(product_id),
    movement_type VARCHAR(20) CHECK (movement_type IN ('IN', 'OUT')),
    quantity INT NOT NULL,
    movement_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- actually Im gonna implement this in the staging area with dbt.
/*CREATE TABLE inventory_balance IF NOT EXISTS (
    product_id INT PRIMARY KEY REFERENCES product(product_id),
    quantity INT NOT NULL,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);*/


GRANT SELECT, INSERT, UPDATE, DELETE
ON ALL TABLES IN SCHEMA cdc_schema
TO cdc_administrator;

GRANT USAGE, SELECT
ON ALL SEQUENCES IN SCHEMA cdc_schema
TO cdc_administrator;

