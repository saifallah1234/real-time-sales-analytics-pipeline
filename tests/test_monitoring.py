import json
import unittest
import urllib.error
from io import BytesIO
from unittest.mock import MagicMock

from airflow_astro.dags.ingestion_logic.ingestion.monitoring import (
    FAIL,
    OK,
    WARN,
    CheckResult,
    connector_checks,
    replication_slot_checks,
    summarize,
    table_checks,
    write_results,
)
from airflow_astro.dags.ingestion_logic.ingestion.tables import TABLES


def opener_returning(payload):
    def opener(request, timeout):
        return BytesIO(json.dumps(payload).encode("utf-8"))

    return opener


def running(name, tasks=1):
    return {
        "status": {
            "name": name,
            "connector": {"state": "RUNNING"},
            "tasks": [{"id": index, "state": "RUNNING"} for index in range(tasks)],
        }
    }


class ConnectorCheckTests(unittest.TestCase):
    def test_unreachable_connect_is_single_failure(self) -> None:
        def opener(request, timeout):
            raise urllib.error.URLError("connection refused")

        results = connector_checks("http://connect:8083", ("a",), opener=opener)

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].check_name, "kafka_connect_reachable")
        self.assertEqual(results[0].status, FAIL)

    def test_running_connector_is_ok_and_missing_expected_connector_fails(self) -> None:
        results = connector_checks(
            "http://connect:8083",
            ("debezium-postgres", "snowflake-cdc-sink"),
            opener=opener_returning({"debezium-postgres": running("debezium-postgres")}),
        )
        by_subject = {r.subject: r for r in results}

        self.assertEqual(by_subject["kafka-connect"].status, OK)
        self.assertEqual(by_subject["debezium-postgres"].status, OK)
        self.assertEqual(by_subject["snowflake-cdc-sink"].status, FAIL)
        self.assertEqual(by_subject["snowflake-cdc-sink"].detail, "connector is not registered")

    def test_failed_task_reports_first_trace_line(self) -> None:
        payload = {
            "snowflake-cdc-sink": {
                "status": {
                    "connector": {"state": "RUNNING"},
                    "tasks": [
                        {
                            "id": 0,
                            "state": "FAILED",
                            "trace": "SnowflakeKafkaConnectorException: auth failed\n\tat x.y",
                        }
                    ],
                }
            }
        }
        [_, result] = connector_checks(
            "http://connect:8083", (), opener=opener_returning(payload)
        )

        self.assertEqual(result.status, FAIL)
        self.assertEqual(result.value, 0.0)
        self.assertIn("task 0=FAILED", result.detail)
        self.assertIn("auth failed", result.detail)
        self.assertNotIn("at x.y", result.detail)

    def test_connector_without_tasks_fails_and_paused_warns(self) -> None:
        payload = {
            "no-tasks": running("no-tasks", tasks=0),
            "paused": {"status": {"connector": {"state": "PAUSED"}, "tasks": [{"id": 0, "state": "PAUSED"}]}},
        }
        results = connector_checks("http://c", (), opener=opener_returning(payload))
        by_subject = {r.subject: r for r in results}

        self.assertEqual(by_subject["no-tasks"].status, FAIL)
        self.assertEqual(by_subject["paused"].status, WARN)


class ReplicationSlotCheckTests(unittest.TestCase):
    def test_slot_states(self) -> None:
        cursor = MagicMock()
        cursor.fetchall.return_value = [
            ("healthy", True, 1024),
            ("lagging", True, 5000),
            ("stopped", False, 10),
            ("invalid", False, None),
        ]

        results = {r.subject: r for r in replication_slot_checks(cursor, warn_bytes=4096)}

        self.assertEqual(results["healthy"].status, OK)
        self.assertEqual(results["healthy"].value, 1024.0)
        self.assertEqual(results["lagging"].status, WARN)
        self.assertEqual(results["stopped"].status, FAIL)
        self.assertEqual(results["invalid"].status, FAIL)

    def test_no_slots_warns(self) -> None:
        cursor = MagicMock()
        cursor.fetchall.return_value = []

        [result] = replication_slot_checks(cursor, warn_bytes=4096)

        self.assertEqual(result.status, WARN)


class TableCheckTests(unittest.TestCase):
    def test_counts_freshness_and_missing_tables(self) -> None:
        cursor = MagicMock()
        columns = {name: True for name in TABLES}
        columns["product"] = False
        del columns["supplier"]
        cursor.fetchall.return_value = list(columns.items())
        rows = {"customer": (0, None), "product": (5, None)}
        cursor.fetchone.side_effect = [
            rows.get(name, (10, 7200.0)) for name in TABLES if name != "supplier"
        ]

        results = table_checks(cursor, "cdc_schema", freshness_warn_seconds=3600)
        by_key = {(r.check_name, r.subject): r for r in results}

        self.assertEqual(by_key[("row_count", "supplier")].status, FAIL)
        self.assertNotIn(("table_freshness", "supplier"), by_key)
        self.assertEqual(by_key[("row_count", "customer")].status, WARN)
        self.assertEqual(by_key[("table_freshness", "customer")].detail, "no rows")
        self.assertIn("03_add_updated_at", by_key[("table_freshness", "product")].detail)
        self.assertEqual(by_key[("row_count", "sales_invoice")].value, 10.0)
        self.assertEqual(by_key[("table_freshness", "sales_invoice")].status, WARN)

    def test_freshness_threshold_zero_disables_warning(self) -> None:
        cursor = MagicMock()
        cursor.fetchall.return_value = [(name, True) for name in TABLES]
        cursor.fetchone.return_value = (3, 10_000_000.0)

        results = table_checks(cursor, "cdc_schema", freshness_warn_seconds=0)

        self.assertTrue(all(r.status == OK for r in results))
        self.assertEqual(len(results), 2 * len(TABLES))


class WriteResultsTests(unittest.TestCase):
    def test_inserts_every_result_and_purges_old_rows(self) -> None:
        cursor = MagicMock()
        results = [
            CheckResult("row_count", "customer", OK, 3.0),
            CheckResult("connector_status", "sink", FAIL, 0.0, "boom"),
        ]

        write_results(cursor, "run-1", results, retention_days=7)

        insert_sql, rows = cursor.executemany.call_args.args
        self.assertIn("INSERT INTO monitoring.check_results", insert_sql)
        self.assertEqual(rows[1], ("run-1", "connector_status", "sink", FAIL, 0.0, "boom"))
        delete_sql, params = cursor.execute.call_args.args
        self.assertIn("DELETE FROM monitoring.check_results", delete_sql)
        self.assertEqual(params, (7,))

    def test_summarize_counts_statuses(self) -> None:
        results = [CheckResult("a", "x", OK), CheckResult("b", "y", FAIL), CheckResult("c", "z", FAIL)]

        self.assertEqual(summarize(results), {OK: 1, WARN: 0, FAIL: 2})


if __name__ == "__main__":
    unittest.main()
