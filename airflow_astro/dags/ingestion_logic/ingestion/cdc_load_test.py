"""End-to-end load test for the PostgreSQL customer CDC pipeline."""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import psycopg
import snowflake.connector

from .config import Settings


FIRST_ID = 900_001
LAST_ID = 901_000
EXPECTED_INSERTS = 1_000
UPDATED_FIRST_ID = 900_001
UPDATED_LAST_ID = 900_100
DELETED_FIRST_ID = 900_101
DELETED_LAST_ID = 900_150


class LoadTestError(RuntimeError):
    """Base error for an unsuccessful load-test run."""


class LoadTestTimeout(LoadTestError):
    """Raised when Snowflake does not converge before the phase deadline."""

    def __init__(self, description: str, observation: dict[str, Any]) -> None:
        self.description = description
        self.observation = observation
        super().__init__(
            f"Timed out waiting for {description}; last observation: "
            f"{_json_safe(observation)}"
        )


class ConnectorHealthError(LoadTestError):
    """Raised when Kafka Connect cannot be monitored or reports a failure."""


@dataclass(frozen=True)
class LoadTestSettings:
    database: Settings
    connect_url: str = "http://localhost:8083"
    landing_schema: str = "CDC_LANDING"
    timeout_seconds: float = 300.0
    poll_interval_seconds: float = 1.0
    cleanup_quiet_seconds: float = 5.0
    report_directory: Path = Path("reports")

    @classmethod
    def from_env(cls) -> "LoadTestSettings":
        return cls(
            database=Settings.from_env(),
            connect_url=os.getenv("KAFKA_CONNECT_URL", "http://localhost:8083").rstrip("/"),
            landing_schema=_identifier(
                os.getenv("LOAD_TEST_LANDING_SCHEMA", "CDC_LANDING")
            ),
            timeout_seconds=_positive_float("LOAD_TEST_TIMEOUT_SECONDS", 300.0),
            poll_interval_seconds=_positive_float(
                "LOAD_TEST_POLL_INTERVAL_SECONDS", 1.0
            ),
            cleanup_quiet_seconds=_positive_float(
                "LOAD_TEST_CLEANUP_QUIET_SECONDS", 5.0
            ),
            report_directory=Path(os.getenv("LOAD_TEST_REPORT_DIR", "reports")),
        )


def _identifier(value: str) -> str:
    if not value or not value.replace("_", "a").isalnum() or value[0].isdigit():
        raise ValueError(f"Invalid unquoted SQL identifier: {value!r}")
    return value


def _positive_float(name: str, default: float) -> float:
    raw = os.getenv(name, str(default))
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a number, got {raw!r}") from exc
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return value


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class ConnectMonitor:
    """Discover and continuously check the source and sink CDC connectors."""

    SOURCE_CLASS = "io.debezium.connector.postgresql.PostgresConnector"
    SINK_CLASS_NAMES = {
        "SnowflakeSinkConnector",
        "SnowflakeStreamingSinkConnector",
    }

    def __init__(
        self,
        base_url: str,
        *,
        opener: Callable[..., Any] = urllib.request.urlopen,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self._opener = opener
        self.connector_names: list[str] = []
        self.failures: dict[str, dict[str, Any]] = {}

    def _get(self, path: str) -> Any:
        request = urllib.request.Request(
            f"{self.base_url}{path}", headers={"Accept": "application/json"}
        )
        try:
            with self._opener(request, timeout=5) as response:
                return json.loads(response.read().decode("utf-8"))
        except (OSError, urllib.error.URLError, json.JSONDecodeError) as exc:
            raise ConnectorHealthError(
                f"Kafka Connect REST request failed for {path}: {exc}"
            ) from exc

    def discover(self) -> list[str]:
        names = self._get("/connectors")
        if not isinstance(names, list):
            raise ConnectorHealthError("Kafka Connect returned an invalid connector list")

        sources: list[str] = []
        sinks: list[str] = []
        for name in names:
            config = self._get(f"/connectors/{name}/config")
            connector_class = str(config.get("connector.class", ""))
            if connector_class == self.SOURCE_CLASS:
                sources.append(name)
            if connector_class.rsplit(".", 1)[-1] in self.SINK_CLASS_NAMES:
                sinks.append(name)

        if not sources or not sinks:
            raise ConnectorHealthError(
                "Expected at least one PostgreSQL Debezium source and one Snowflake "
                f"sink; discovered sources={sources}, sinks={sinks}"
            )
        self.connector_names = sources + sinks
        self.check(require_running=True)
        return list(self.connector_names)

    def check(self, *, require_running: bool = True) -> None:
        if not self.connector_names:
            raise ConnectorHealthError("Connector discovery has not run")

        unhealthy: list[str] = []
        for name in self.connector_names:
            status = self._get(f"/connectors/{name}/status")
            components = [("connector", status.get("connector", {}))]
            tasks = status.get("tasks", [])
            if not tasks:
                unhealthy.append(f"{name}/tasks=NONE")
            components.extend(
                (f"task:{task.get('id')}", task) for task in tasks
            )
            for component, details in components:
                state = str(details.get("state", "UNKNOWN")).upper()
                identity = f"{name}/{component}"
                if state == "FAILED":
                    self.failures.setdefault(
                        identity,
                        {
                            "connector": name,
                            "component": component,
                            "state": state,
                            "trace": details.get("trace"),
                            "observed_at": _utc_now(),
                        },
                    )
                if state != "RUNNING":
                    unhealthy.append(f"{identity}={state}")

        if self.failures:
            raise ConnectorHealthError(
                "Kafka Connect failure observed: " + ", ".join(self.failures)
            )
        if require_running and unhealthy:
            raise ConnectorHealthError(
                "Kafka Connect components are not RUNNING: " + ", ".join(unhealthy)
            )


def _qualified(database: str, schema: str, table: str) -> str:
    return ".".join(_identifier(part) for part in (database, schema, table))


def _integer(value: Any) -> int:
    """Normalize nullable aggregate and driver row-count values."""
    return int(value or 0)


def _raw_snapshot(sf_connection: Any, raw_table: str, marker: str) -> dict[str, Any]:
    cursor = sf_connection.cursor()
    try:
        cursor.execute(
            f"""SELECT COUNT(*), COUNT(DISTINCT CUSTOMER_ID),
                       COALESCE(COUNT_IF(FIRST_NAME = %s), 0),
                       COALESCE(COUNT_IF(CUSTOMER_ID BETWEEN %s AND %s), 0)
                FROM {raw_table}
                WHERE CUSTOMER_ID BETWEEN %s AND %s""",
            (
                marker,
                DELETED_FIRST_ID,
                DELETED_LAST_ID,
                FIRST_ID,
                LAST_ID,
            ),
        )
        total, distinct, updated, deleted_remaining = cursor.fetchone()
        cursor.execute(
            f"""SELECT CUSTOMER_ID
                FROM {raw_table}
                WHERE CUSTOMER_ID BETWEEN %s AND %s
                GROUP BY CUSTOMER_ID""",
            (FIRST_ID, LAST_ID),
        )
        present = {int(row[0]) for row in cursor.fetchall()}
    finally:
        cursor.close()

    missing = sorted(set(range(FIRST_ID, LAST_ID + 1)) - present)
    expected_survivors = set(range(FIRST_ID, LAST_ID + 1)) - set(
        range(DELETED_FIRST_ID, DELETED_LAST_ID + 1)
    )
    missing_survivors = sorted(expected_survivors - present)
    return {
        "total_records": _integer(total),
        "distinct_ids": _integer(distinct),
        "duplicate_primary_keys": _integer(total) - _integer(distinct),
        "updated_records": _integer(updated),
        "deleted_records_remaining": _integer(deleted_remaining),
        "missing_count": len(missing),
        "missing_ids_sample": missing[:25],
        "missing_surviving_count": len(missing_survivors),
        "missing_surviving_ids_sample": missing_survivors[:25],
        "present_ids": present,
    }


def _public_snapshot(snapshot: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in snapshot.items() if key != "present_ids"}


def _landing_counts(sf_connection: Any, landing_table: str) -> dict[str, int]:
    cursor = sf_connection.cursor()
    try:
        cursor.execute(
            f"""SELECT COALESCE(__OP, '<NULL>'), COUNT(*)
                FROM {landing_table}
                WHERE CUSTOMER_ID BETWEEN %s AND %s
                GROUP BY 1""",
            (FIRST_ID, LAST_ID),
        )
        return {
            str(operation): _integer(count) for operation, count in cursor.fetchall()
        }
    finally:
        cursor.close()


def _count_delta(current: dict[str, int], baseline: dict[str, int]) -> dict[str, int]:
    keys = current.keys() | baseline.keys()
    return {key: current.get(key, 0) - baseline.get(key, 0) for key in sorted(keys)}


def _wait_for(
    description: str,
    predicate: Callable[[], tuple[bool, dict[str, Any]]],
    monitor: ConnectMonitor,
    settings: LoadTestSettings,
) -> dict[str, Any]:
    deadline = time.monotonic() + settings.timeout_seconds
    last_observation: dict[str, Any] = {}
    while True:
        monitor.check()
        complete, last_observation = predicate()
        if complete:
            return last_observation
        if time.monotonic() >= deadline:
            raise LoadTestTimeout(description, last_observation)
        time.sleep(settings.poll_interval_seconds)


def _cleanup(
    pg_connection: Any,
    sf_connection: Any,
    raw_table: str,
    landing_table: str,
    monitor: ConnectMonitor,
    settings: LoadTestSettings,
) -> dict[str, Any]:
    initial_landing = _landing_counts(sf_connection, landing_table)
    with pg_connection.cursor() as cursor:
        cursor.execute(
            f"DELETE FROM {_identifier(settings.database.pg_schema)}.customer "
            "WHERE customer_id BETWEEN %s AND %s",
            (FIRST_ID, LAST_ID),
        )
        postgres_deleted = _integer(cursor.rowcount)
    pg_connection.commit()

    cursor = sf_connection.cursor()
    try:
        cursor.execute(
            f"DELETE FROM {raw_table} WHERE CUSTOMER_ID BETWEEN %s AND %s",
            (FIRST_ID, LAST_ID),
        )
        snowflake_deleted = _integer(cursor.rowcount)
    finally:
        cursor.close()
    sf_connection.commit()

    stable_since: float | None = None
    previous_counts: dict[str, int] | None = None

    def cleanup_complete() -> tuple[bool, dict[str, Any]]:
        nonlocal stable_since, previous_counts
        snapshot = _raw_snapshot(sf_connection, raw_table, "")
        counts = _landing_counts(sf_connection, landing_table)
        now = time.monotonic()
        if counts != previous_counts:
            previous_counts = counts
            stable_since = now
        delete_delta = counts.get("d", 0) - initial_landing.get("d", 0)
        quiet = stable_since is not None and now - stable_since >= settings.cleanup_quiet_seconds
        complete = (
            snapshot["total_records"] == 0
            and delete_delta >= postgres_deleted
            and quiet
        )
        return complete, {
            "raw_records": snapshot["total_records"],
            "landing_counts": counts,
            "cleanup_delete_delta": delete_delta,
            "quiet": quiet,
        }

    final = _wait_for("cleanup convergence", cleanup_complete, monitor, settings)
    return {
        "postgres_deleted": postgres_deleted,
        "snowflake_deleted": snowflake_deleted,
        "observation": final,
    }


def _insert_customers(pg_connection: Any, pg_schema: str) -> int:
    with pg_connection.cursor() as cursor:
        cursor.execute(
            f"""INSERT INTO {_identifier(pg_schema)}.customer
                   (customer_id, first_name, last_name, email, phone_number)
               SELECT customer_id,
                      'LoadTest',
                      'Customer' || customer_id,
                      'cdc-load-' || customer_id || '@example.test',
                      '+1999' || LPAD(customer_id::text, 10, '0')
               FROM generate_series(%s, %s) AS generated(customer_id)""",
            (FIRST_ID, LAST_ID),
        )
        affected = _integer(cursor.rowcount)
    pg_connection.commit()
    return affected


def _apply_mixed_changes(
    pg_connection: Any, pg_schema: str, marker: str
) -> tuple[int, int]:
    with pg_connection.cursor() as cursor:
        cursor.execute(
            f"""UPDATE {_identifier(pg_schema)}.customer
               SET first_name = %s
               WHERE customer_id BETWEEN %s AND %s""",
            (marker, UPDATED_FIRST_ID, UPDATED_LAST_ID),
        )
        updated = _integer(cursor.rowcount)
    pg_connection.commit()

    with pg_connection.cursor() as cursor:
        cursor.execute(
            f"DELETE FROM {_identifier(pg_schema)}.customer "
            "WHERE customer_id BETWEEN %s AND %s",
            (DELETED_FIRST_ID, DELETED_LAST_ID),
        )
        deleted = _integer(cursor.rowcount)
    pg_connection.commit()
    return updated, deleted


def _insert_complete(snapshot: dict[str, Any]) -> bool:
    return (
        snapshot["total_records"] == EXPECTED_INSERTS
        and snapshot["distinct_ids"] == EXPECTED_INSERTS
        and snapshot["missing_count"] == 0
        and snapshot["duplicate_primary_keys"] == 0
    )


def _mixed_complete(snapshot: dict[str, Any]) -> bool:
    expected_present = set(range(FIRST_ID, LAST_ID + 1)) - set(
        range(DELETED_FIRST_ID, DELETED_LAST_ID + 1)
    )
    return (
        snapshot["total_records"] == 950
        and snapshot["distinct_ids"] == 950
        and snapshot["duplicate_primary_keys"] == 0
        and snapshot["updated_records"] == 100
        and snapshot["deleted_records_remaining"] == 0
        and snapshot["present_ids"] == expected_present
    )


def _assertion(name: str, actual: Any, expected: Any) -> dict[str, Any]:
    return {
        "name": name,
        "actual": actual,
        "expected": expected,
        "passed": actual == expected,
    }


def _json_safe(value: Any) -> Any:
    if isinstance(value, set):
        return sorted(value)
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return value


def run_load_test(settings: LoadTestSettings | None = None) -> dict[str, Any]:
    settings = settings or LoadTestSettings.from_env()
    run_id = (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        + "-"
        + uuid.uuid4().hex[:8]
    )
    marker = f"LOADTEST_UPDATED_{run_id[-8:]}"
    report: dict[str, Any] = {
        "run_id": run_id,
        "status": "FAILED",
        "reserved_range": {"first": FIRST_ID, "last": LAST_ID},
        "update_range": {"first": UPDATED_FIRST_ID, "last": UPDATED_LAST_ID},
        "delete_range": {"first": DELETED_FIRST_ID, "last": DELETED_LAST_ID},
        "connector_failures": [],
        "assertions": [],
    }
    monitor = ConnectMonitor(settings.connect_url)
    pg_connection = None
    sf_connection = None

    database = settings.database.snowflake_database
    raw_table = _qualified(database, settings.database.snowflake_schema, "CUSTOMER")
    landing_table = _qualified(database, settings.landing_schema, "CUSTOMER_CHANGES")

    try:
        report["connectors"] = monitor.discover()
        pg_connection = psycopg.connect(**settings.database.postgres_connect_kwargs())
        sf_connection = snowflake.connector.connect(
            **settings.database.snowflake_connect_kwargs()
        )

        report["cleanup"] = _cleanup(
            pg_connection,
            sf_connection,
            raw_table,
            landing_table,
            monitor,
            settings,
        )
        baseline = _landing_counts(sf_connection, landing_table)
        report["landing_baseline"] = baseline

        insert_started_at = _utc_now()
        insert_started_ns = time.perf_counter_ns()
        report["insert_phase"] = {"start_time": insert_started_at}
        inserted = _insert_customers(pg_connection, settings.database.pg_schema)
        report["insert_phase"]["postgres_inserted_records"] = inserted

        def inserts_arrived() -> tuple[bool, dict[str, Any]]:
            snapshot = _raw_snapshot(sf_connection, raw_table, marker)
            return _insert_complete(snapshot), snapshot

        insert_snapshot = _wait_for(
            "1,000 inserts in RAW_DATA.CUSTOMER", inserts_arrived, monitor, settings
        )
        insert_ended_ns = time.perf_counter_ns()
        insert_ended_at = _utc_now()
        insert_seconds = max(insert_ended_ns - insert_started_ns, 1) / 1_000_000_000
        report["insert_phase"].update(
            {
                "end_time": insert_ended_at,
                "total_seconds": insert_seconds,
                "snowflake": _public_snapshot(insert_snapshot),
                "throughput_records_per_second": EXPECTED_INSERTS / insert_seconds,
                "throughput_label": "observed laptop end-to-end throughput",
            }
        )

        mixed_started_at = _utc_now()
        mixed_started_ns = time.perf_counter_ns()
        report["mixed_phase"] = {"start_time": mixed_started_at}
        updated, deleted = _apply_mixed_changes(
            pg_connection, settings.database.pg_schema, marker
        )
        report["mixed_phase"].update(
            {
                "postgres_updated_records": updated,
                "postgres_deleted_records": deleted,
            }
        )

        def mixed_arrived() -> tuple[bool, dict[str, Any]]:
            snapshot = _raw_snapshot(sf_connection, raw_table, marker)
            return _mixed_complete(snapshot), snapshot

        mixed_snapshot = _wait_for(
            "mixed changes in RAW_DATA.CUSTOMER", mixed_arrived, monitor, settings
        )
        mixed_seconds = max(
            time.perf_counter_ns() - mixed_started_ns, 1
        ) / 1_000_000_000
        landing_final = _landing_counts(sf_connection, landing_table)
        landing_delta = _count_delta(landing_final, baseline)
        total_cdc = sum(landing_delta.values())
        report["mixed_phase"].update(
            {
                "end_time": _utc_now(),
                "total_seconds": mixed_seconds,
                "snowflake": _public_snapshot(mixed_snapshot),
            }
        )
        report["landing_final"] = landing_final
        report["landing_delta"] = landing_delta
        report["landing_total_cdc_changes"] = total_cdc

        assertions = [
            _assertion("PostgreSQL inserted records", inserted, 1000),
            _assertion(
                "Snowflake inserted records", insert_snapshot["total_records"], 1000
            ),
            _assertion("Missing inserted records", insert_snapshot["missing_count"], 0),
            _assertion(
                "Duplicate inserted primary keys",
                insert_snapshot["duplicate_primary_keys"],
                0,
            ),
            _assertion("PostgreSQL updated records", updated, 100),
            _assertion("PostgreSQL deleted records", deleted, 50),
            _assertion("Snowflake final records", mixed_snapshot["total_records"], 950),
            _assertion(
                "Missing surviving records",
                mixed_snapshot["missing_surviving_count"],
                0,
            ),
            _assertion(
                "Updated values in Snowflake", mixed_snapshot["updated_records"], 100
            ),
            _assertion(
                "Deleted records remaining",
                mixed_snapshot["deleted_records_remaining"],
                0,
            ),
            _assertion(
                "Final duplicate primary keys",
                mixed_snapshot["duplicate_primary_keys"],
                0,
            ),
            _assertion("Landing insert changes", landing_delta.get("c", 0), 1000),
            _assertion("Landing update changes", landing_delta.get("u", 0), 100),
            _assertion("Landing delete changes", landing_delta.get("d", 0), 50),
            _assertion("Landing total CDC changes", total_cdc, 1150),
            _assertion("Connector failures", len(monitor.failures), 0),
        ]
        report["assertions"] = assertions
        report["status"] = "PASSED" if all(item["passed"] for item in assertions) else "FAILED"
    except Exception as exc:
        if pg_connection is not None:
            try:
                pg_connection.rollback()
            except Exception:
                pass
        report["error"] = {"type": type(exc).__name__, "message": str(exc)}
        if isinstance(exc, LoadTestTimeout):
            report["timeout"] = {
                "waiting_for": exc.description,
                "last_observation": _json_safe(exc.observation),
            }
    finally:
        report["connector_failures"] = list(monitor.failures.values())
        if pg_connection is not None:
            pg_connection.close()
        if sf_connection is not None:
            sf_connection.close()

    return _json_safe(report)


def write_report(report: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def print_summary(report: dict[str, Any]) -> None:
    print(f"CDC load test: {report['status']} ({report['run_id']})")
    if "snowflake" in report.get("insert_phase", {}):
        phase = report["insert_phase"]
        print(
            f"Insert: {phase['snowflake']['total_records']}/1000 records, "
            f"{phase['total_seconds']:.3f}s, "
            f"{phase['throughput_records_per_second']:.2f} records/second"
        )
    elif "insert_phase" in report:
        print("Insert: did not reach Snowflake convergence")
    if "snowflake" in report.get("mixed_phase", {}):
        phase = report["mixed_phase"]
        print(
            f"Mixed final: {phase['snowflake']['total_records']}/950 records, "
            f"updated={phase['snowflake']['updated_records']}, "
            f"deleted_remaining={phase['snowflake']['deleted_records_remaining']}"
        )
        print(f"Landing CDC changes: {report.get('landing_total_cdc_changes', 0)}/1150")
    elif "mixed_phase" in report:
        print("Mixed final: did not reach Snowflake convergence")
    print(f"Connector failures: {len(report.get('connector_failures', []))}")
    if "error" in report:
        print(f"Error: {report['error']['type']}: {report['error']['message']}")
    failed = [item for item in report.get("assertions", []) if not item["passed"]]
    for item in failed:
        print(
            f"FAILED: {item['name']} expected={item['expected']} actual={item['actual']}"
        )
