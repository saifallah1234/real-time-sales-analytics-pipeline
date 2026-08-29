import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, patch

from airflow_astro.dags.ingestion_logic.ingestion.config import Settings
from airflow_astro.dags.ingestion_logic.ingestion.pipeline import (
    IngestionAlreadyRunningError,
    PostgreSQLMigrationRequiredError,
    TableResult,
    _merge_counts,
    _merge_table,
    _process_table,
    _validate_postgres_schema,
    run_ingestion,
)
from airflow_astro.dags.ingestion_logic.ingestion.tables import TABLES


def settings() -> Settings:
    return Settings(
        pg_host="localhost",
        pg_port=5432,
        pg_database="cdc_project",
        pg_user="saif",
        pg_password="secret",
        pg_schema="cdc_schema",
        snowflake_account="account",
        snowflake_user="user",
        snowflake_password="secret",
        snowflake_warehouse="warehouse",
        snowflake_role=None,
        snowflake_database="CDC_DATABASE",
        snowflake_schema="RAW_DATA",
        batch_size=100,
        overlap_minutes=5,
        advisory_lock_id=123,
        log_level="INFO",
    )


class MergeMetricTests(unittest.TestCase):
    def test_merge_counts_distinguish_inserted_and_updated(self) -> None:
        cursor = SimpleNamespace(
            description=[("number of rows inserted",), ("number of rows updated",)],
            fetchone=lambda: (4, 2),
        )
        self.assertEqual(_merge_counts(cursor), (4, 2))

    def test_merge_and_checkpoint_share_one_transaction(self) -> None:
        cursor = Mock()
        cursor.fetchone.return_value = (3, 1)
        cursor.description = [
            ("number of rows inserted",),
            ("number of rows updated",),
        ]
        sf_connection = Mock()
        sf_connection.cursor.return_value = cursor
        cutoff = datetime(2026, 8, 24, 10, tzinfo=timezone.utc)

        result = _merge_table(
            sf_connection,
            settings(),
            TABLES["customer"],
            "TMP_CDC_CUSTOMER",
            cutoff,
            8,
        )

        self.assertEqual(result, (3, 1))
        statements = [entry.args[0] for entry in cursor.execute.call_args_list]
        self.assertEqual(statements[0], "BEGIN")
        self.assertTrue(statements[1].startswith("MERGE INTO"))
        self.assertIn("INGESTION_CHECKPOINTS", statements[2])
        self.assertEqual(
            cursor.execute.call_args_list[2].args[1],
            ("customer", cutoff, 8, 3, 1),
        )
        self.assertEqual(statements[3], "COMMIT")

    def test_failed_merge_rolls_back_before_checkpoint(self) -> None:
        cursor = Mock()
        cursor.execute.side_effect = [None, RuntimeError("merge failed"), None]
        sf_connection = Mock()
        sf_connection.cursor.return_value = cursor

        with self.assertRaisesRegex(RuntimeError, "merge failed"):
            _merge_table(
                sf_connection,
                settings(),
                TABLES["customer"],
                "TMP_CDC_CUSTOMER",
                datetime.now(timezone.utc),
                1,
            )

        self.assertEqual(
            [entry.args[0] for entry in cursor.execute.call_args_list][-1], "ROLLBACK"
        )


class WindowTests(unittest.TestCase):
    @patch("ingestion.pipeline._merge_table", return_value=(0, 0))
    @patch("ingestion.pipeline._extract_to_stage", return_value=0)
    @patch("ingestion.pipeline._create_stage", return_value="TMP_CDC_PRODUCT")
    @patch("ingestion.pipeline._capture_upper_bound")
    @patch("ingestion.pipeline._read_checkpoint")
    def test_empty_run_advances_to_exact_upper_bound_with_overlap(
        self,
        read_checkpoint: Mock,
        capture_upper_bound: Mock,
        create_stage: Mock,
        extract_to_stage: Mock,
        merge_table: Mock,
    ) -> None:
        previous = datetime(2026, 8, 24, 9, tzinfo=timezone.utc)
        upper = datetime(2026, 8, 24, 10, tzinfo=timezone.utc)
        read_checkpoint.return_value = previous
        capture_upper_bound.return_value = upper
        pg_connection = Mock()
        sf_connection = Mock()

        result = _process_table(
            pg_connection,
            sf_connection,
            settings(),
            TABLES["product"],
        )

        self.assertEqual(result.successful_cutoff, upper)
        self.assertEqual(result.extracted_row_count, 0)
        extract_args = extract_to_stage.call_args.args
        self.assertEqual(extract_args[-2], previous - timedelta(minutes=5))
        self.assertEqual(extract_args[-1], upper)
        self.assertEqual(merge_table.call_args.args[-2:], (upper, 0))
        create_stage.assert_called_once()
        extract_to_stage.assert_called_once()
        pg_connection.commit.assert_called_once_with()


class AdvisoryLockTests(unittest.TestCase):
    @patch("ingestion.pipeline._process_table")
    @patch("ingestion.pipeline._setup_snowflake")
    @patch("ingestion.pipeline._validate_postgres_schema")
    @patch("ingestion.pipeline.snowflake.connector.connect")
    @patch("ingestion.pipeline.psycopg.connect")
    def test_session_lock_is_held_until_selected_tables_finish(
        self,
        pg_connect: Mock,
        sf_connect: Mock,
        validate_postgres_schema: Mock,
        setup_snowflake: Mock,
        process_table: Mock,
    ) -> None:
        pg_connection = MagicMock()
        pg_cursor = pg_connection.cursor.return_value.__enter__.return_value
        pg_cursor.fetchone.return_value = (True,)
        pg_connect.return_value = pg_connection
        sf_connection = Mock()
        sf_connect.return_value = sf_connection
        cutoff = datetime(2026, 8, 24, 10, tzinfo=timezone.utc)
        process_table.return_value = TableResult("customer", cutoff, 1, 1, 0)

        results = run_ingestion(["customer"], settings=settings())

        self.assertEqual(results[0].table, "customer")
        setup_snowflake.assert_called_once()
        validate_postgres_schema.assert_called_once()
        process_table.assert_called_once()
        executed_sql = [item.args[0] for item in pg_cursor.execute.call_args_list]
        self.assertEqual(
            executed_sql,
            ["SELECT pg_try_advisory_lock(%s)", "SELECT pg_advisory_unlock(%s)"],
        )
        pg_connection.close.assert_called_once_with()
        sf_connection.close.assert_called_once_with()
        pg_connection.rollback.assert_called_once_with()

    @patch("ingestion.pipeline.snowflake.connector.connect")
    @patch("ingestion.pipeline.psycopg.connect")
    def test_unavailable_lock_fails_before_opening_snowflake(
        self, pg_connect: Mock, sf_connect: Mock
    ) -> None:
        pg_connection = MagicMock()
        pg_cursor = pg_connection.cursor.return_value.__enter__.return_value
        pg_cursor.fetchone.return_value = (False,)
        pg_connect.return_value = pg_connection

        with self.assertRaises(IngestionAlreadyRunningError):
            run_ingestion(["customer"], settings=settings())

        sf_connect.assert_not_called()
        pg_connection.close.assert_called_once_with()


class PostgreSQLPreflightTests(unittest.TestCase):
    def test_missing_updated_at_columns_produce_migration_instruction(self) -> None:
        pg_connection = MagicMock()
        cursor = pg_connection.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = [("customer",)]

        with self.assertRaisesRegex(
            PostgreSQLMigrationRequiredError,
            "postgres/init/03_add_updated_at.sql",
        ):
            _validate_postgres_schema(
                pg_connection,
                settings(),
                [TABLES["customer"], TABLES["supplier"]],
            )


if __name__ == "__main__":
    unittest.main()
