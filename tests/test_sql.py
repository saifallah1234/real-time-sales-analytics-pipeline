import unittest

from airflow_astro.dags.ingestion_logic.ingestion.sql import postgres_extract_sql, snowflake_merge_sql
from airflow_astro.dags.ingestion_logic.ingestion.tables import TABLES


class SqlContractTests(unittest.TestCase):
    def test_first_run_has_only_fixed_upper_bound(self) -> None:
        sql = postgres_extract_sql(TABLES["customer"], "cdc_schema", True)
        self.assertIn("WHERE updated_at <= %(upper_bound)s", sql)
        self.assertNotIn("lower_bound", sql)
        self.assertTrue(sql.endswith("ORDER BY updated_at, customer_id"))

    def test_incremental_window_uses_exact_bounds_and_order(self) -> None:
        sql = postgres_extract_sql(TABLES["product"], "cdc_schema", False)
        self.assertIn("updated_at > %(lower_bound)s", sql)
        self.assertIn("updated_at <= %(upper_bound)s", sql)
        self.assertTrue(sql.endswith("ORDER BY updated_at, product_id"))

    def test_merge_deduplicates_and_only_accepts_newer_records(self) -> None:
        sql = snowflake_merge_sql(
            TABLES["inventory_movement"],
            "CDC_DATABASE.RAW_DATA.INVENTORY_MOVEMENT",
            "TMP_CDC_INVENTORY_MOVEMENT",
        )
        self.assertIn("PARTITION BY movement_id", sql)
        self.assertIn("ORDER BY updated_at DESC", sql)
        self.assertIn("QUALIFY ROW_NUMBER()", sql)
        self.assertIn("target.updated_at IS NULL", sql)
        self.assertIn("source.updated_at > target.updated_at", sql)
        self.assertNotIn("THEN DELETE", sql)


if __name__ == "__main__":
    unittest.main()
