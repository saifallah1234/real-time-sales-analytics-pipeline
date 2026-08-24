\connect cdc_project

SET search_path TO cdc_schema;

BEGIN;

-- Add the column first, then establish its default/backfill/constraint safely.
ALTER TABLE customer ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;
ALTER TABLE supplier ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;
ALTER TABLE product ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;
ALTER TABLE sales_invoice ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;
ALTER TABLE sales_invoice_item ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;
ALTER TABLE purchase_bill ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;
ALTER TABLE purchase_bill_item ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;
ALTER TABLE inventory_movement ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;

ALTER TABLE customer ALTER COLUMN updated_at SET DEFAULT clock_timestamp();
ALTER TABLE supplier ALTER COLUMN updated_at SET DEFAULT clock_timestamp();
ALTER TABLE product ALTER COLUMN updated_at SET DEFAULT clock_timestamp();
ALTER TABLE sales_invoice ALTER COLUMN updated_at SET DEFAULT clock_timestamp();
ALTER TABLE sales_invoice_item ALTER COLUMN updated_at SET DEFAULT clock_timestamp();
ALTER TABLE purchase_bill ALTER COLUMN updated_at SET DEFAULT clock_timestamp();
ALTER TABLE purchase_bill_item ALTER COLUMN updated_at SET DEFAULT clock_timestamp();
ALTER TABLE inventory_movement ALTER COLUMN updated_at SET DEFAULT clock_timestamp();

UPDATE customer SET updated_at = clock_timestamp() WHERE updated_at IS NULL;
UPDATE supplier SET updated_at = clock_timestamp() WHERE updated_at IS NULL;
UPDATE product SET updated_at = clock_timestamp() WHERE updated_at IS NULL;
UPDATE sales_invoice SET updated_at = clock_timestamp() WHERE updated_at IS NULL;
UPDATE sales_invoice_item SET updated_at = clock_timestamp() WHERE updated_at IS NULL;
UPDATE purchase_bill SET updated_at = clock_timestamp() WHERE updated_at IS NULL;
UPDATE purchase_bill_item SET updated_at = clock_timestamp() WHERE updated_at IS NULL;
UPDATE inventory_movement SET updated_at = clock_timestamp() WHERE updated_at IS NULL;

ALTER TABLE customer ALTER COLUMN updated_at SET NOT NULL;
ALTER TABLE supplier ALTER COLUMN updated_at SET NOT NULL;
ALTER TABLE product ALTER COLUMN updated_at SET NOT NULL;
ALTER TABLE sales_invoice ALTER COLUMN updated_at SET NOT NULL;
ALTER TABLE sales_invoice_item ALTER COLUMN updated_at SET NOT NULL;
ALTER TABLE purchase_bill ALTER COLUMN updated_at SET NOT NULL;
ALTER TABLE purchase_bill_item ALTER COLUMN updated_at SET NOT NULL;
ALTER TABLE inventory_movement ALTER COLUMN updated_at SET NOT NULL;

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.updated_at := clock_timestamp();
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS set_customer_updated_at ON customer;
CREATE TRIGGER set_customer_updated_at
BEFORE UPDATE ON customer
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS set_supplier_updated_at ON supplier;
CREATE TRIGGER set_supplier_updated_at
BEFORE UPDATE ON supplier
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS set_product_updated_at ON product;
CREATE TRIGGER set_product_updated_at
BEFORE UPDATE ON product
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS set_sales_invoice_updated_at ON sales_invoice;
CREATE TRIGGER set_sales_invoice_updated_at
BEFORE UPDATE ON sales_invoice
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS set_sales_invoice_item_updated_at ON sales_invoice_item;
CREATE TRIGGER set_sales_invoice_item_updated_at
BEFORE UPDATE ON sales_invoice_item
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS set_purchase_bill_updated_at ON purchase_bill;
CREATE TRIGGER set_purchase_bill_updated_at
BEFORE UPDATE ON purchase_bill
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS set_purchase_bill_item_updated_at ON purchase_bill_item;
CREATE TRIGGER set_purchase_bill_item_updated_at
BEFORE UPDATE ON purchase_bill_item
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS set_inventory_movement_updated_at ON inventory_movement;
CREATE TRIGGER set_inventory_movement_updated_at
BEFORE UPDATE ON inventory_movement
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

COMMIT;
