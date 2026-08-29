import json
import tempfile
import unittest
from contextlib import redirect_stdout
from io import BytesIO
from io import StringIO
from pathlib import Path
from unittest.mock import MagicMock, patch

from airflow_astro.dags.ingestion_logic.ingestion import cdc_load_test

from airflow_astro.dags.ingestion_logic.ingestion.cdc_load_test import (
    DELETED_FIRST_ID,
    DELETED_LAST_ID,
    FIRST_ID,
    LAST_ID,
    ConnectMonitor,
    ConnectorHealthError,
    LoadTestTimeout,
    LoadTestSettings,
    _count_delta,
    _insert_complete,
    _mixed_complete,
    _public_snapshot,
    _raw_snapshot,
    _wait_for,
    print_summary,
    write_report,
)
from airflow_astro.dags.ingestion_logic.ingestion.config import Settings


class FakeResponse:
    def __init__(self, payload):
        self.body = BytesIO(json.dumps(payload).encode("utf-8"))

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def read(self):
        return self.body.read()


def opener_for(routes):
    def open_request(request, timeout):
        del timeout
        path = request.full_url.removeprefix("http://connect:8083")
        return FakeResponse(routes[path])

    return open_request


class ConnectorMonitorTests(unittest.TestCase):
    def test_discovers_source_and_sink_and_requires_running_tasks(self):
        routes = {
            "/connectors": ["source", "sink", "unrelated"],
            "/connectors/source/config": {
                "connector.class": "io.debezium.connector.postgresql.PostgresConnector"
            },
            "/connectors/sink/config": {
                "connector.class": "com.snowflake.kafka.connector.SnowflakeStreamingSinkConnector"
            },
            "/connectors/unrelated/config": {"connector.class": "example.Other"},
            "/connectors/source/status": {
                "connector": {"state": "RUNNING"},
                "tasks": [{"id": 0, "state": "RUNNING"}],
            },
            "/connectors/sink/status": {
                "connector": {"state": "RUNNING"},
                "tasks": [{"id": 0, "state": "RUNNING"}],
            },
        }
        monitor = ConnectMonitor(
            "http://connect:8083", opener=opener_for(routes)
        )

        self.assertEqual(monitor.discover(), ["source", "sink"])
        self.assertEqual(monitor.failures, {})

    def test_records_failed_task_trace(self):
        routes = {
            "/connectors/source/status": {
                "connector": {"state": "RUNNING"},
                "tasks": [{"id": 0, "state": "FAILED", "trace": "bad WAL"}],
            }
        }
        monitor = ConnectMonitor(
            "http://connect:8083", opener=opener_for(routes)
        )
        monitor.connector_names = ["source"]

        with self.assertRaisesRegex(ConnectorHealthError, "source/task:0"):
            monitor.check()

        failure = monitor.failures["source/task:0"]
        self.assertEqual(failure["trace"], "bad WAL")
        self.assertEqual(failure["state"], "FAILED")


class StateEvaluationTests(unittest.TestCase):
    def test_empty_snowflake_range_converts_count_if_nulls_to_zero(self):
        cursor = MagicMock()
        cursor.fetchone.return_value = (0, 0, None, None)
        cursor.fetchall.return_value = []
        connection = MagicMock()
        connection.cursor.return_value = cursor

        snapshot = _raw_snapshot(connection, "DATABASE.RAW_DATA.CUSTOMER", "marker")

        self.assertEqual(snapshot["total_records"], 0)
        self.assertEqual(snapshot["updated_records"], 0)
        self.assertEqual(snapshot["deleted_records_remaining"], 0)
        sql = cursor.execute.call_args_list[0].args[0]
        self.assertIn("COALESCE(COUNT_IF(FIRST_NAME = %s), 0)", sql)

    def test_insert_completion_requires_exact_unique_range(self):
        complete = {
            "total_records": 1000,
            "distinct_ids": 1000,
            "missing_count": 0,
            "duplicate_primary_keys": 0,
        }
        self.assertTrue(_insert_complete(complete))
        self.assertFalse(_insert_complete({**complete, "distinct_ids": 999}))
        self.assertFalse(_insert_complete({**complete, "missing_count": 1}))

    def test_mixed_completion_uses_disjoint_update_and_delete_ranges(self):
        survivors = set(range(FIRST_ID, LAST_ID + 1)) - set(
            range(DELETED_FIRST_ID, DELETED_LAST_ID + 1)
        )
        snapshot = {
            "total_records": 950,
            "distinct_ids": 950,
            "duplicate_primary_keys": 0,
            "updated_records": 100,
            "deleted_records_remaining": 0,
            "present_ids": survivors,
        }
        self.assertTrue(_mixed_complete(snapshot))
        self.assertFalse(
            _mixed_complete({**snapshot, "present_ids": survivors - {FIRST_ID}})
        )

    def test_landing_counts_are_reported_as_baseline_deltas(self):
        self.assertEqual(
            _count_delta(
                {"c": 1100, "u": 130, "d": 70},
                {"c": 100, "u": 30, "d": 20},
            ),
            {"c": 1000, "d": 50, "u": 100},
        )

    def test_public_snapshot_removes_large_internal_id_set(self):
        self.assertEqual(
            _public_snapshot({"total_records": 1, "present_ids": {FIRST_ID}}),
            {"total_records": 1},
        )

    def test_timeout_preserves_last_observation(self):
        monitor = MagicMock()
        database = Settings(
            pg_host="localhost",
            pg_port=5432,
            pg_database="database",
            pg_user="user",
            pg_password="password",
            pg_schema="cdc_schema",
            snowflake_account="account",
            snowflake_user="user",
            snowflake_password="password",
            snowflake_warehouse="warehouse",
            snowflake_role=None,
            snowflake_database="database",
            snowflake_schema="raw_data",
            batch_size=1000,
            overlap_minutes=5,
            advisory_lock_id=1,
            log_level="INFO",
        )
        settings = LoadTestSettings(
            database=database,
            timeout_seconds=0.001,
            poll_interval_seconds=0.001,
            cleanup_quiet_seconds=0.001,
        )

        with self.assertRaises(LoadTestTimeout) as raised:
            _wait_for(
                "records",
                lambda: (False, {"total_records": 999, "missing_count": 1}),
                monitor,
                settings,
            )

        self.assertEqual(raised.exception.description, "records")
        self.assertEqual(raised.exception.observation["missing_count"], 1)


class ReportTests(unittest.TestCase):
    def test_json_report_is_created(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "nested" / "report.json"
            write_report({"status": "PASSED", "run_id": "run"}, path)
            self.assertEqual(
                json.loads(path.read_text()), {"run_id": "run", "status": "PASSED"}
            )

    def test_partial_failed_phase_has_a_readable_summary(self):
        output = StringIO()
        report = {
            "status": "FAILED",
            "run_id": "run",
            "insert_phase": {"start_time": "now"},
            "connector_failures": [],
            "assertions": [],
            "error": {"type": "LoadTestTimeout", "message": "not converged"},
        }
        with redirect_stdout(output):
            print_summary(report)

        self.assertIn("did not reach Snowflake convergence", output.getvalue())
        self.assertIn("LoadTestTimeout", output.getvalue())

    def test_happy_path_builds_complete_passing_report(self):
        database = Settings(
            pg_host="localhost",
            pg_port=5432,
            pg_database="database",
            pg_user="user",
            pg_password="password",
            pg_schema="cdc_schema",
            snowflake_account="account",
            snowflake_user="user",
            snowflake_password="password",
            snowflake_warehouse="warehouse",
            snowflake_role=None,
            snowflake_database="database",
            snowflake_schema="raw_data",
            batch_size=1000,
            overlap_minutes=5,
            advisory_lock_id=1,
            log_level="INFO",
        )
        settings = LoadTestSettings(
            database=database,
            timeout_seconds=1,
            poll_interval_seconds=0.001,
            cleanup_quiet_seconds=0.001,
        )
        all_ids = set(range(FIRST_ID, LAST_ID + 1))
        survivors = all_ids - set(range(DELETED_FIRST_ID, DELETED_LAST_ID + 1))
        insert_snapshot = {
            "total_records": 1000,
            "distinct_ids": 1000,
            "duplicate_primary_keys": 0,
            "updated_records": 0,
            "deleted_records_remaining": 50,
            "missing_count": 0,
            "missing_ids_sample": [],
            "missing_surviving_count": 0,
            "missing_surviving_ids_sample": [],
            "present_ids": all_ids,
        }
        mixed_snapshot = {
            "total_records": 950,
            "distinct_ids": 950,
            "duplicate_primary_keys": 0,
            "updated_records": 100,
            "deleted_records_remaining": 0,
            "missing_count": 50,
            "missing_ids_sample": list(range(DELETED_FIRST_ID, DELETED_LAST_ID + 1))[:25],
            "missing_surviving_count": 0,
            "missing_surviving_ids_sample": [],
            "present_ids": survivors,
        }
        monitor = MagicMock()
        monitor.discover.return_value = ["source", "sink"]
        monitor.failures = {}

        with (
            patch.object(cdc_load_test, "ConnectMonitor", return_value=monitor),
            patch.object(cdc_load_test.psycopg, "connect", return_value=MagicMock()),
            patch.object(
                cdc_load_test.snowflake.connector,
                "connect",
                return_value=MagicMock(),
            ),
            patch.object(cdc_load_test, "_cleanup", return_value={}),
            patch.object(
                cdc_load_test,
                "_landing_counts",
                side_effect=[
                    {"c": 10, "u": 10, "d": 10},
                    {"c": 1010, "u": 110, "d": 60},
                ],
            ),
            patch.object(cdc_load_test, "_insert_customers", return_value=1000),
            patch.object(cdc_load_test, "_apply_mixed_changes", return_value=(100, 50)),
            patch.object(
                cdc_load_test,
                "_raw_snapshot",
                side_effect=[insert_snapshot, mixed_snapshot],
            ),
        ):
            report = cdc_load_test.run_load_test(settings)

        self.assertEqual(report["status"], "PASSED", report)
        self.assertEqual(report["landing_total_cdc_changes"], 1150)
        self.assertEqual(report["landing_delta"], {"c": 1000, "d": 50, "u": 100})
        self.assertGreater(
            report["insert_phase"]["throughput_records_per_second"], 0
        )
        self.assertTrue(all(item["passed"] for item in report["assertions"]))


if __name__ == "__main__":
    unittest.main()
