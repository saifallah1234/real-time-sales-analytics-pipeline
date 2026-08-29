import unittest

from airflow_astro.dags.ingestion_logic.ingestion.tables import TABLES, select_tables


class TableRegistryTests(unittest.TestCase):
    def test_registry_contains_exactly_the_eight_source_tables(self) -> None:
        self.assertEqual(
            list(TABLES),
            [
                "customer",
                "supplier",
                "product",
                "sales_invoice",
                "sales_invoice_item",
                "purchase_bill",
                "purchase_bill_item",
                "inventory_movement",
            ],
        )
        self.assertEqual(
            {name: spec.primary_key for name, spec in TABLES.items()},
            {
                "customer": "customer_id",
                "supplier": "supplier_id",
                "product": "product_id",
                "sales_invoice": "invoice_id",
                "sales_invoice_item": "item_id",
                "purchase_bill": "bill_id",
                "purchase_bill_item": "item_id",
                "inventory_movement": "movement_id",
            },
        )

    def test_selection_rejects_unknown_tables_and_removes_duplicates(self) -> None:
        self.assertEqual(
            [spec.name for spec in select_tables(["product", "product", "customer"])],
            ["product", "customer"],
        )
        with self.assertRaisesRegex(ValueError, "Unknown table"):
            select_tables(["inventory"])


if __name__ == "__main__":
    unittest.main()
