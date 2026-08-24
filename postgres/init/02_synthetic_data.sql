-- Synthetic data for cdc_schema.
-- Run after init_01.sql against the cdc_project database.
-- Assumes the target tables are empty.

\connect cdc_project

SET search_path TO cdc_schema;

BEGIN;

-- ---------------------------------------------------------------------------
-- Parent tables: 10 customers, 10 suppliers, 20 products
-- ---------------------------------------------------------------------------

INSERT INTO customer
    (customer_id, first_name, last_name, email, phone_number, created_at)
VALUES
    (1,  'Amira',   'Benali',    'amira.benali@example.test',   '+33610000001', TIMESTAMP '2025-08-15 09:00:00'),
    (2,  'Lucas',   'Martin',    'lucas.martin@example.test',   '+33610000002', TIMESTAMP '2025-08-17 10:15:00'),
    (3,  'Sofia',   'Rossi',     'sofia.rossi@example.test',    '+33610000003', TIMESTAMP '2025-08-20 14:30:00'),
    (4,  'Noah',    'Dubois',    'noah.dubois@example.test',    '+33610000004', TIMESTAMP '2025-08-22 08:45:00'),
    (5,  'Emma',    'Schmidt',   'emma.schmidt@example.test',   '+33610000005', TIMESTAMP '2025-08-25 11:20:00'),
    (6,  'Youssef', 'Haddad',    'youssef.haddad@example.test', '+33610000006', TIMESTAMP '2025-08-27 16:10:00'),
    (7,  'Maya',    'Petrov',    'maya.petrov@example.test',    '+33610000007', TIMESTAMP '2025-08-28 12:00:00'),
    (8,  'Hugo',    'Lefevre',   'hugo.lefevre@example.test',   '+33610000008', TIMESTAMP '2025-08-29 09:40:00'),
    (9,  'Ines',    'Garcia',    'ines.garcia@example.test',    '+33610000009', TIMESTAMP '2025-08-30 15:25:00'),
    (10, 'Adam',    'Kowalski',  'adam.kowalski@example.test',  '+33610000010', TIMESTAMP '2025-08-31 17:50:00');

INSERT INTO supplier
    (supplier_id, name, contact_name, contact_email, contact_phone, created_at)
VALUES
    (1,  'Alpine Office Supply', 'Claire Moreau',  'orders@alpine-office.example.test', '+33120000001', TIMESTAMP '2025-08-01 09:00:00'),
    (2,  'Nordic Tech Parts',    'Erik Lund',       'sales@nordic-tech.example.test',    '+33120000002', TIMESTAMP '2025-08-02 09:15:00'),
    (3,  'GreenPack Europe',     'Laura Vidal',     'hello@greenpack.example.test',      '+33120000003', TIMESTAMP '2025-08-03 10:30:00'),
    (4,  'Metro Electronics',    'Karim Mansour',   'trade@metro-electronics.example.test', '+33120000004', TIMESTAMP '2025-08-04 11:00:00'),
    (5,  'Atlas Furniture',      'Nadia Rahal',     'b2b@atlas-furniture.example.test',  '+33120000005', TIMESTAMP '2025-08-05 13:20:00'),
    (6,  'BlueLine Logistics',   'Thomas Bernard',  'supply@blueline.example.test',      '+33120000006', TIMESTAMP '2025-08-06 14:10:00'),
    (7,  'Pixel Accessories',    'Giulia Conti',    'orders@pixel-accessories.example.test', '+33120000007', TIMESTAMP '2025-08-07 15:00:00'),
    (8,  'Central Paper Mill',   'Jan Novak',       'export@central-paper.example.test', '+33120000008', TIMESTAMP '2025-08-08 09:50:00'),
    (9,  'Lumina Lighting',      'Ana Costa',       'sales@lumina.example.test',         '+33120000009', TIMESTAMP '2025-08-09 12:45:00'),
    (10, 'ErgoWorks',            'Marc Petit',      'contact@ergoworks.example.test',    '+33120000010', TIMESTAMP '2025-08-10 16:30:00');

INSERT INTO product
    (product_id, name, description, unit_price, created_at)
VALUES
    (1,  'A4 Copy Paper',        '500-sheet recycled paper ream',                 6.90, TIMESTAMP '2025-08-12 08:00:00'),
    (2,  'Ballpoint Pen Set',    'Pack of ten blue ballpoint pens',               8.50, TIMESTAMP '2025-08-12 08:05:00'),
    (3,  'Desk Notebook',        'Hardcover ruled notebook, 160 pages',          12.00, TIMESTAMP '2025-08-12 08:10:00'),
    (4,  'USB-C Cable',          'Two-metre braided charging cable',             14.90, TIMESTAMP '2025-08-12 08:15:00'),
    (5,  'Wireless Mouse',       'Compact rechargeable wireless mouse',          29.90, TIMESTAMP '2025-08-12 08:20:00'),
    (6,  'Mechanical Keyboard',  'Low-profile mechanical office keyboard',       79.00, TIMESTAMP '2025-08-12 08:25:00'),
    (7,  'Laptop Stand',         'Adjustable aluminium laptop stand',            49.50, TIMESTAMP '2025-08-12 08:30:00'),
    (8,  'Webcam HD',            '1080p webcam with privacy shutter',             59.90, TIMESTAMP '2025-08-12 08:35:00'),
    (9,  'Noise-Cancel Headset', 'USB headset with boom microphone',             89.00, TIMESTAMP '2025-08-12 08:40:00'),
    (10, 'LED Desk Lamp',        'Dimmable LED lamp with USB port',               42.00, TIMESTAMP '2025-08-12 08:45:00'),
    (11, 'Monitor 24 Inch',      'Full-HD IPS business monitor',                 169.00, TIMESTAMP '2025-08-12 08:50:00'),
    (12, 'Monitor Arm',          'Single gas-spring monitor arm',                 74.90, TIMESTAMP '2025-08-12 08:55:00'),
    (13, 'Office Chair',         'Ergonomic mesh office chair',                  249.00, TIMESTAMP '2025-08-12 09:00:00'),
    (14, 'Standing Desk',        'Electric height-adjustable desk',              499.00, TIMESTAMP '2025-08-12 09:05:00'),
    (15, 'Cable Organizer',      'Reusable cable-management sleeve',               9.90, TIMESTAMP '2025-08-12 09:10:00'),
    (16, 'Portable SSD 1TB',     'USB-C solid-state portable drive',             119.00, TIMESTAMP '2025-08-12 09:15:00'),
    (17, 'Power Strip',          'Six-outlet surge-protected power strip',        24.50, TIMESTAMP '2025-08-12 09:20:00'),
    (18, 'Whiteboard',           'Magnetic 90 x 60 cm whiteboard',                65.00, TIMESTAMP '2025-08-12 09:25:00'),
    (19, 'Sticky Notes Pack',    'Twelve assorted-colour note pads',             11.50, TIMESTAMP '2025-08-12 09:30:00'),
    (20, 'Document Shredder',    'Cross-cut ten-sheet document shredder',        135.00, TIMESTAMP '2025-08-12 09:35:00');

-- ---------------------------------------------------------------------------
-- Sales: 50 invoices across almost 12 months, then exactly 100 line items.
-- total_sum is the line subtotal after discounts; tax_amount is 20% of it.
-- ---------------------------------------------------------------------------

INSERT INTO sales_invoice
    (invoice_id, customer_id, invoice_date, total_sum, tax_amount, status, created_at)
SELECT
    g,
    ((g - 1) % 10) + 1,
    TIMESTAMP '2025-09-01 10:00:00' + ((g - 1) * INTERVAL '7 days'),
    ROUND(line_totals.subtotal, 2),
    ROUND(line_totals.subtotal * 0.20, 2),
    CASE
        WHEN g % 7 = 0 THEN 'Cancelled'
        WHEN g % 5 = 0 THEN 'Overdue'
        WHEN g % 3 = 0 THEN 'Paid'
        ELSE 'Pending'
    END,
    TIMESTAMP '2025-09-01 10:05:00' + ((g - 1) * INTERVAL '7 days')
FROM generate_series(1, 50) AS series(g)
CROSS JOIN LATERAL (
    SELECT SUM((p.unit_price * line.quantity) - line.discount)::NUMERIC AS subtotal
    FROM (VALUES
        (((g - 1) % 20) + 1, (g % 4) + 1, CASE WHEN g % 3 = 0 THEN 5.00::NUMERIC ELSE 0.00::NUMERIC END),
        (((g + 6) % 20) + 1, ((g + 1) % 3) + 1, CASE WHEN g % 4 = 0 THEN 2.50::NUMERIC ELSE 0.00::NUMERIC END)
    ) AS line(product_id, quantity, discount)
    JOIN product p ON p.product_id = line.product_id
) AS line_totals;

INSERT INTO sales_invoice_item
    (item_id, invoice_id, product_id, quantity, unit_price, discount, created_at)
SELECT
    ((g - 1) * 2) + line.line_number,
    g,
    line.product_id,
    line.quantity,
    p.unit_price,
    line.discount,
    TIMESTAMP '2025-09-01 11:00:00' + ((g - 1) * INTERVAL '7 days')
FROM generate_series(1, 50) AS series(g)
CROSS JOIN LATERAL (VALUES
    (1, ((g - 1) % 20) + 1, (g % 4) + 1, CASE WHEN g % 3 = 0 THEN 5.00::NUMERIC ELSE 0.00::NUMERIC END),
    (2, ((g + 6) % 20) + 1, ((g + 1) % 3) + 1, CASE WHEN g % 4 = 0 THEN 2.50::NUMERIC ELSE 0.00::NUMERIC END)
) AS line(line_number, product_id, quantity, discount)
JOIN product p ON p.product_id = line.product_id;

-- ---------------------------------------------------------------------------
-- Purchasing: 50 bills across almost 12 months, then exactly 100 line items.
-- Unit cost is deliberately below retail price. total_sum excludes 20% tax.
-- ---------------------------------------------------------------------------

INSERT INTO purchase_bill
    (bill_id, supplier_id, bill_date, total_sum, tax_amount, paid_amount, status, created_at)
SELECT
    g,
    ((g - 1) % 10) + 1,
    TIMESTAMP '2025-08-28 08:00:00' + ((g - 1) * INTERVAL '7 days'),
    ROUND(line_totals.subtotal, 2),
    ROUND(line_totals.subtotal * 0.20, 2),
    CASE
        WHEN g % 6 = 0 THEN 0.00
        WHEN g % 4 = 0 THEN ROUND(line_totals.subtotal * 1.20, 2)
        WHEN g % 3 = 0 THEN ROUND(line_totals.subtotal * 0.60, 2)
        ELSE 0.00
    END,
    CASE
        WHEN g % 6 = 0 THEN 'Cancelled'
        WHEN g % 4 = 0 THEN 'Paid'
        WHEN g % 3 = 0 THEN 'Partially Paid'
        ELSE 'Pending'
    END,
    TIMESTAMP '2025-08-28 08:05:00' + ((g - 1) * INTERVAL '7 days')
FROM generate_series(1, 50) AS series(g)
CROSS JOIN LATERAL (
    SELECT SUM(line.unit_cost * line.quantity)::NUMERIC AS subtotal
    FROM (VALUES
        (ROUND((SELECT unit_price FROM product WHERE product_id = ((g + 2) % 20) + 1) * 0.58, 2), (g % 11) + 10),
        (ROUND((SELECT unit_price FROM product WHERE product_id = ((g + 10) % 20) + 1) * 0.62, 2), ((g + 3) % 7) + 8)
    ) AS line(unit_cost, quantity)
) AS line_totals;

INSERT INTO purchase_bill_item
    (item_id, bill_id, product_id, quantity, unit_price, created_at)
SELECT
    ((g - 1) * 2) + line.line_number,
    g,
    line.product_id,
    line.quantity,
    ROUND(p.unit_price * line.cost_factor, 2),
    TIMESTAMP '2025-08-28 09:00:00' + ((g - 1) * INTERVAL '7 days')
FROM generate_series(1, 50) AS series(g)
CROSS JOIN LATERAL (VALUES
    (1, ((g + 2) % 20) + 1, (g % 11) + 10, 0.58::NUMERIC),
    (2, ((g + 10) % 20) + 1, ((g + 3) % 7) + 8, 0.62::NUMERIC)
) AS line(line_number, product_id, quantity, cost_factor)
JOIN product p ON p.product_id = line.product_id;

-- ---------------------------------------------------------------------------
-- Inventory: one IN for every purchase line and one OUT for every sales line.
-- This creates 200 movements with both movement types across all 20 products.
-- ---------------------------------------------------------------------------

INSERT INTO inventory_movement
    (movement_id, product_id, movement_type, quantity, movement_date, created_at)
SELECT
    pbi.item_id,
    pbi.product_id,
    'IN',
    pbi.quantity,
    pb.bill_date + INTERVAL '2 hours',
    pb.bill_date + INTERVAL '2 hours 5 minutes'
FROM purchase_bill_item pbi
JOIN purchase_bill pb ON pb.bill_id = pbi.bill_id;

INSERT INTO inventory_movement
    (movement_id, product_id, movement_type, quantity, movement_date, created_at)
SELECT
    100 + sii.item_id,
    sii.product_id,
    'OUT',
    sii.quantity,
    si.invoice_date + INTERVAL '2 hours',
    si.invoice_date + INTERVAL '2 hours 5 minutes'
FROM sales_invoice_item sii
JOIN sales_invoice si ON si.invoice_id = sii.invoice_id;

-- ---------------------------------------------------------------------------
-- CDC exercise events.
-- These leave all requested final row counts unchanged.
-- ---------------------------------------------------------------------------

-- UPDATE events: customer profile, price, invoice status, and bill settlement.
UPDATE customer
SET phone_number = '+33619990001'
WHERE customer_id = 1;

UPDATE product
SET unit_price = 139.00,
    description = 'Cross-cut ten-sheet shredder with updated safety guard'
WHERE product_id = 20;

UPDATE sales_invoice
SET status = 'Paid'
WHERE invoice_id = 2;

UPDATE purchase_bill
SET status = 'Paid',
    paid_amount = total_sum + tax_amount
WHERE bill_id = 2;

-- DELETE plus replacement INSERT events, preserving two lines per document.
DELETE FROM sales_invoice_item
WHERE item_id = 100;

INSERT INTO sales_invoice_item
    (item_id, invoice_id, product_id, quantity, unit_price, discount, created_at)
SELECT
    101, 50, 17, 1, p.unit_price, 0.00, TIMESTAMP '2026-08-10 11:10:00'
FROM product p
WHERE p.product_id = 17;

DELETE FROM purchase_bill_item
WHERE item_id = 100;

INSERT INTO purchase_bill_item
    (item_id, bill_id, product_id, quantity, unit_price, created_at)
SELECT
    101, 50, 1, 12, ROUND(p.unit_price * 0.62, 2), TIMESTAMP '2026-08-06 09:10:00'
FROM product p
WHERE p.product_id = 1;

DELETE FROM inventory_movement
WHERE movement_id = 200;

INSERT INTO inventory_movement
    (movement_id, product_id, movement_type, quantity, movement_date, created_at)
VALUES
    (201, 17, 'OUT', 1, TIMESTAMP '2026-08-10 12:00:00', TIMESTAMP '2026-08-10 12:05:00');

-- Explicit IDs were used for reproducibility; advance every SERIAL sequence.
SELECT setval(pg_get_serial_sequence('customer', 'customer_id'),
              (SELECT MAX(customer_id) FROM customer), true);
SELECT setval(pg_get_serial_sequence('supplier', 'supplier_id'),
              (SELECT MAX(supplier_id) FROM supplier), true);
SELECT setval(pg_get_serial_sequence('product', 'product_id'),
              (SELECT MAX(product_id) FROM product), true);
SELECT setval(pg_get_serial_sequence('sales_invoice', 'invoice_id'),
              (SELECT MAX(invoice_id) FROM sales_invoice), true);
SELECT setval(pg_get_serial_sequence('sales_invoice_item', 'item_id'),
              (SELECT MAX(item_id) FROM sales_invoice_item), true);
SELECT setval(pg_get_serial_sequence('purchase_bill', 'bill_id'),
              (SELECT MAX(bill_id) FROM purchase_bill), true);
SELECT setval(pg_get_serial_sequence('purchase_bill_item', 'item_id'),
              (SELECT MAX(item_id) FROM purchase_bill_item), true);
SELECT setval(pg_get_serial_sequence('inventory_movement', 'movement_id'),
              (SELECT MAX(movement_id) FROM inventory_movement), true);

COMMIT;

-- Optional verification query (expected: 10, 10, 20, 50, 100, 50, 100, 200).
SELECT
    (SELECT COUNT(*) FROM customer) AS customers,
    (SELECT COUNT(*) FROM supplier) AS suppliers,
    (SELECT COUNT(*) FROM product) AS products,
    (SELECT COUNT(*) FROM sales_invoice) AS sales_invoices,
    (SELECT COUNT(*) FROM sales_invoice_item) AS sales_invoice_items,
    (SELECT COUNT(*) FROM purchase_bill) AS purchase_bills,
    (SELECT COUNT(*) FROM purchase_bill_item) AS purchase_bill_items,
    (SELECT COUNT(*) FROM inventory_movement) AS inventory_movements;
