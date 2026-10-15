"""Pipeline health checks recorded in PostgreSQL for the Grafana dashboard.

The collector needs only the source PostgreSQL database and the Kafka Connect
REST API, so it keeps running while Snowflake is unreachable; a broken
Snowflake sink then shows up as a failing connector check.
"""

from __future__ import annotations

import json
import os
import urllib.request
import uuid
from dataclasses import dataclass
from typing import Any, Callable

import psycopg
from dotenv import find_dotenv, load_dotenv
from psycopg import sql

from .config import _identifier, _positive_int, _required
from .tables import TABLES


RESULTS_TABLE = "monitoring.check_results"
OK, WARN, FAIL = "ok", "warn", "fail"
_DETAIL_LIMIT = 500


class MonitoringSchemaMissingError(RuntimeError):
    """Raised when postgres/init/04_monitoring.sql has not been applied."""


@dataclass(frozen=True)
class CheckResult:
    check_name: str
    subject: str
    status: str
    value: float | None = None
    detail: str | None = None


@dataclass(frozen=True)
class MonitorSettings:
    pg_host: str
    pg_port: int
    pg_database: str
    pg_user: str
    pg_password: str
    pg_schema: str
    connect_url: str
    expected_connectors: tuple[str, ...]
    slot_lag_warn_bytes: int
    freshness_warn_seconds: int
    retention_days: int

    @classmethod
    def from_env(cls) -> "MonitorSettings":
        # Search from the working directory: the default search starts at this
        # module and would pick up airflow_astro/.env (container hostnames).
        load_dotenv(find_dotenv(usecwd=True))
        expected = os.getenv(
            "MONITOR_EXPECTED_CONNECTORS", "debezium-postgres,snowflake-cdc-sink"
        )
        return cls(
            pg_host=os.getenv("PG_HOST", "localhost"),
            pg_port=_positive_int("PG_PORT", 5432),
            pg_database=os.getenv("PG_DATABASE", "cdc_project"),
            pg_user=os.getenv("PG_USER", "saif"),
            pg_password=_required("PG_PASSWORD"),
            pg_schema=_identifier("PG_SCHEMA", "cdc_schema"),
            connect_url=os.getenv("KAFKA_CONNECT_URL", "http://localhost:8083").rstrip("/"),
            expected_connectors=tuple(
                name.strip() for name in expected.split(",") if name.strip()
            ),
            slot_lag_warn_bytes=_positive_int(
                "MONITOR_SLOT_LAG_WARN_BYTES", 100 * 1024 * 1024
            ),
            # 0 disables the freshness threshold; the age is still recorded.
            freshness_warn_seconds=_positive_int(
                "MONITOR_FRESHNESS_WARN_SECONDS", 0, allow_zero=True
            ),
            retention_days=_positive_int("MONITOR_RETENTION_DAYS", 7),
        )

    def postgres_connect_kwargs(self) -> dict[str, object]:
        return {
            "host": self.pg_host,
            "port": self.pg_port,
            "dbname": self.pg_database,
            "user": self.pg_user,
            "password": self.pg_password,
            "connect_timeout": 5,
            "application_name": "cdc_project_monitor",
        }


def _truncate(text: str) -> str:
    return text if len(text) <= _DETAIL_LIMIT else text[: _DETAIL_LIMIT - 3] + "..."


def connector_checks(
    connect_url: str,
    expected: tuple[str, ...],
    *,
    opener: Callable[..., Any] = urllib.request.urlopen,
) -> list[CheckResult]:
    request = urllib.request.Request(
        f"{connect_url}/connectors?expand=status",
        headers={"Accept": "application/json"},
    )
    try:
        with opener(request, timeout=5) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return [
            CheckResult(
                "kafka_connect_reachable", "kafka-connect", FAIL, 0.0, _truncate(str(exc))
            )
        ]
    if not isinstance(payload, dict):
        return [
            CheckResult(
                "kafka_connect_reachable",
                "kafka-connect",
                FAIL,
                0.0,
                "Kafka Connect returned an unexpected response",
            )
        ]

    results = [CheckResult("kafka_connect_reachable", "kafka-connect", OK, 1.0)]
    for name in sorted(set(expected) | set(payload)):
        status = (payload.get(name) or {}).get("status")
        if status is None:
            results.append(
                CheckResult("connector_status", name, FAIL, 0.0, "connector is not registered")
            )
        else:
            results.append(_connector_result(name, status))
    return results


def _connector_result(name: str, status: dict[str, Any]) -> CheckResult:
    components = [("connector", status.get("connector") or {})]
    components += [(f"task {task.get('id')}", task) for task in status.get("tasks") or []]
    states = [
        (label, str(details.get("state", "UNKNOWN")).upper(), details.get("trace"))
        for label, details in components
    ]

    detail = "; ".join(f"{label}={state}" for label, state, _ in states)
    if len(components) == 1:
        detail += "; no tasks"
    traces = [trace.strip().splitlines()[0] for _, _, trace in states if trace]
    if traces:
        # The first trace line holds the exception class and message.
        detail += f" | {traces[0]}"

    if len(components) == 1 or any(state == "FAILED" for _, state, _ in states):
        outcome = FAIL
    elif all(state == "RUNNING" for _, state, _ in states):
        outcome = OK
    else:
        outcome = WARN
    return CheckResult(
        "connector_status", name, outcome, 1.0 if outcome == OK else 0.0, _truncate(detail)
    )


_SLOT_SQL = """
SELECT
    slot_name,
    active,
    pg_wal_lsn_diff(pg_current_wal_lsn(), COALESCE(confirmed_flush_lsn, restart_lsn))
FROM pg_replication_slots
WHERE slot_type = 'logical'
ORDER BY slot_name
"""


def replication_slot_checks(cursor: Any, warn_bytes: int) -> list[CheckResult]:
    cursor.execute(_SLOT_SQL)
    rows = cursor.fetchall()
    if not rows:
        return [
            CheckResult(
                "replication_slot_lag",
                "(none)",
                WARN,
                None,
                "no logical replication slots; is the Debezium connector registered?",
            )
        ]

    results = []
    for slot_name, active, lag in rows:
        if lag is None:
            results.append(
                CheckResult(
                    "replication_slot_lag", slot_name, FAIL, None, "slot has no LSN (invalidated?)"
                )
            )
            continue
        lag_bytes = float(lag)
        if not active:
            outcome, detail = FAIL, "slot is inactive; PostgreSQL retains WAL until it is consumed"
        elif lag_bytes > warn_bytes:
            outcome, detail = WARN, f"lag exceeds {warn_bytes} bytes"
        else:
            outcome, detail = OK, None
        results.append(CheckResult("replication_slot_lag", slot_name, outcome, lag_bytes, detail))
    return results


_TABLE_COLUMNS_SQL = """
SELECT table_name, bool_or(column_name = 'updated_at')
FROM information_schema.columns
WHERE table_schema = %s
GROUP BY table_name
"""


def table_checks(cursor: Any, schema: str, freshness_warn_seconds: int) -> list[CheckResult]:
    cursor.execute(_TABLE_COLUMNS_SQL, (schema,))
    has_updated_at = dict(cursor.fetchall())

    results = []
    for spec in TABLES.values():
        if spec.name not in has_updated_at:
            results.append(
                CheckResult("row_count", spec.name, FAIL, None, f"table {schema}.{spec.name} not found")
            )
            continue

        table = sql.Identifier(schema, spec.name)
        if has_updated_at[spec.name]:
            query = sql.SQL(
                "SELECT count(*), EXTRACT(EPOCH FROM now() - max(updated_at)) FROM {}"
            ).format(table)
        else:
            query = sql.SQL("SELECT count(*), NULL FROM {}").format(table)
        cursor.execute(query)
        row_count, age = cursor.fetchone()

        results.append(
            CheckResult(
                "row_count",
                spec.name,
                OK if row_count else WARN,
                float(row_count),
                None if row_count else "table is empty",
            )
        )
        results.append(
            _freshness_result(spec.name, has_updated_at[spec.name], age, freshness_warn_seconds)
        )
    return results


def _freshness_result(
    table: str, has_updated_at: bool, age: Any, warn_seconds: int
) -> CheckResult:
    if not has_updated_at:
        return CheckResult(
            "table_freshness",
            table,
            WARN,
            None,
            "updated_at column missing; apply postgres/init/03_add_updated_at.sql",
        )
    if age is None:
        return CheckResult("table_freshness", table, WARN, None, "no rows")
    age_seconds = float(age)
    if warn_seconds and age_seconds > warn_seconds:
        return CheckResult(
            "table_freshness",
            table,
            WARN,
            age_seconds,
            f"no update for more than {warn_seconds} seconds",
        )
    return CheckResult("table_freshness", table, OK, age_seconds)


def write_results(
    cursor: Any, run_id: uuid.UUID, results: list[CheckResult], retention_days: int
) -> None:
    cursor.executemany(
        f"""INSERT INTO {RESULTS_TABLE}
            (run_id, check_name, subject, status, value, detail)
        VALUES (%s, %s, %s, %s, %s, %s)""",
        [
            (run_id, r.check_name, r.subject, r.status, r.value, r.detail)
            for r in results
        ],
    )
    cursor.execute(
        f"DELETE FROM {RESULTS_TABLE} WHERE checked_at < now() - make_interval(days => %s)",
        (retention_days,),
    )


def run_monitoring(
    settings: MonitorSettings | None = None,
) -> tuple[uuid.UUID, list[CheckResult]]:
    settings = settings or MonitorSettings.from_env()
    run_id = uuid.uuid4()
    results = connector_checks(settings.connect_url, settings.expected_connectors)

    with psycopg.connect(**settings.postgres_connect_kwargs()) as connection:
        with connection.cursor() as cursor:
            results += replication_slot_checks(cursor, settings.slot_lag_warn_bytes)
            results += table_checks(
                cursor, settings.pg_schema, settings.freshness_warn_seconds
            )
            try:
                write_results(cursor, run_id, results, settings.retention_days)
            except psycopg.errors.UndefinedTable as exc:
                raise MonitoringSchemaMissingError(
                    f"{RESULTS_TABLE} does not exist; apply postgres/init/04_monitoring.sql"
                ) from exc
    return run_id, results


def summarize(results: list[CheckResult]) -> dict[str, int]:
    counts = {OK: 0, WARN: 0, FAIL: 0}
    for result in results:
        counts[result.status] += 1
    return counts
