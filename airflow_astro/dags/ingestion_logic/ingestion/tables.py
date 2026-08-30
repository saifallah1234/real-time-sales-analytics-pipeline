from dataclasses import dataclass


@dataclass(frozen=True) #hedha bech ykhalik t3ml instance mta3 class TableSpec immutable, ma tnajmch tbadl l attributes mta3ha ba3d ma t3mlha instance

class TableSpec:
    name: str
    primary_key: str
    columns: tuple[str, ...]

    @property
    def all_columns(self) -> tuple[str, ...]:
        return (*self.columns, "updated_at") # hedha yaani ("columns_id", "name",...,"updated_at")


TABLES: dict[str, TableSpec] = {
    "customer": TableSpec(
        "customer",
        "customer_id",
        (
            "customer_id",
            "first_name",
            "last_name",
            "email",
            "phone_number",
            "created_at",
        ),
    ),
    "supplier": TableSpec(
        "supplier",
        "supplier_id",
        (
            "supplier_id",
            "name",
            "contact_name",
            "contact_email",
            "contact_phone",
            "created_at",
        ),
    ),
    "product": TableSpec(
        "product",
        "product_id",
        (
            "product_id",
            "name",
            "description",
            "unit_price",
            "created_at",
        ),
    ),
    "sales_invoice": TableSpec(
        "sales_invoice",
        "invoice_id",
        (
            "invoice_id",
            "customer_id",
            "invoice_date",
            "total_sum",
            "tax_amount",
            "status",
            "created_at",
        ),
    ),
    "sales_invoice_item": TableSpec(
        "sales_invoice_item",
        "item_id",
        (
            "item_id",
            "invoice_id",
            "product_id",
            "quantity",
            "unit_price",
            "discount",
            "created_at",
        ),
    ),
    "purchase_bill": TableSpec(
        "purchase_bill",
        "bill_id",
        (
            "bill_id",
            "supplier_id",
            "bill_date",
            "total_sum",
            "tax_amount",
            "paid_amount",
            "status",
            "created_at",
        ),
    ),
    "purchase_bill_item": TableSpec(
        "purchase_bill_item",
        "item_id",
        (
            "item_id",
            "bill_id",
            "product_id",
            "quantity",
            "unit_price",
            "created_at",
        ),
    ),
    "inventory_movement": TableSpec(
        "inventory_movement",
        "movement_id",
        (
            "movement_id",
            "product_id",
            "movement_type",
            "quantity",
            "movement_date",
            "created_at",
        ),
    ),
}


def select_tables(table_names: list[str] | tuple[str, ...] | None) -> list[TableSpec]:
    if table_names is None:
        return list(TABLES.values())

    unknown = sorted(set(table_names) - TABLES.keys())
    if unknown:
        raise ValueError(
            f"Unknown table(s): {', '.join(unknown)}. "
            f"Allowed tables: {', '.join(TABLES)}"
        )
    # Preserve CLI order but never process the same table twice in one run.
    return [TABLES[name] for name in dict.fromkeys(table_names)]
