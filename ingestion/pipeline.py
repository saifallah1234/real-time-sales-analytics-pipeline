import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import psycopg
import snowflake.connector

from ingestion.config import Settings
from ingestion.sql import (
    CHECKPOINT_TABLE,
    checkpoint_merge_sql,
    postgres_extract_sql,
    snowflake_merge_sql,
)
from ingestion.tables import TableSpec, select_tables


LOGGER = logging.getLogger(__name__)


class IngestionAlreadyRunningError(RuntimeError):
    """Raised when another process owns the session advisory lock."""


class PostgreSQLMigrationRequiredError(RuntimeError):
    """Raised when the source tables do not have the CDC timestamp migration."""


@dataclass(frozen=True)
class TableResult:
    table: str
    successful_cutoff: datetime
    extracted_row_count: int
    inserted_row_count: int
    updated_row_count: int


def _qualified(settings: Settings, object_name: str) -> str:
    return (
        f"{settings.snowflake_database}."
        f"{settings.snowflake_schema}.{object_name.upper()}"
    )


def _setup_snowflake(sf_connection: Any, settings: Settings, specs: list[TableSpec]) -> None:
    checkpoint = _qualified(settings, CHECKPOINT_TABLE)
    cursor = sf_connection.cursor()
    try:
        cursor.execute(
            f"""CREATE TABLE IF NOT EXISTS {checkpoint} (
                source_table VARCHAR NOT NULL,
                successful_cutoff TIMESTAMP_TZ NOT NULL,
                extracted_row_count NUMBER NOT NULL,
                inserted_row_count NUMBER NOT NULL,
                updated_row_count NUMBER NOT NULL,
                ingested_at TIMESTAMP_TZ NOT NULL
            )"""
        )
        # These ALTERs make setup idempotent if a partial/older checkpoint exists.
        checkpoint_columns = (
            ("SUCCESSFUL_CUTOFF", "TIMESTAMP_TZ"),
            ("EXTRACTED_ROW_COUNT", "NUMBER"),
            ("INSERTED_ROW_COUNT", "NUMBER"),
            ("UPDATED_ROW_COUNT", "NUMBER"),
            ("INGESTED_AT", "TIMESTAMP_TZ"),
        )
        for column, data_type in checkpoint_columns:
            cursor.execute(
                f"ALTER TABLE {checkpoint} ADD COLUMN IF NOT EXISTS {column} {data_type}"
            )
        for spec in specs:
            cursor.execute(
                f"ALTER TABLE {_qualified(settings, spec.name)} "
                "ADD COLUMN IF NOT EXISTS UPDATED_AT TIMESTAMP_TZ"
            )
    finally:
        cursor.close()


def _validate_postgres_schema(
    pg_connection: Any, settings: Settings, specs: list[TableSpec]
) -> None:
    """Fail before Snowflake work if the updated_at migration is missing."""
    expected = {spec.name for spec in specs}
    with pg_connection.cursor() as cursor:
        cursor.execute(
            """SELECT table_name
            FROM information_schema.columns
            WHERE table_schema = %s
              AND column_name = 'updated_at'""",
            (settings.pg_schema,),
        )
        tables_with_column = {row[0] for row in cursor.fetchall()}

    missing = sorted(expected - tables_with_column)
    if missing:
        raise PostgreSQLMigrationRequiredError(
            "PostgreSQL updated_at migration is missing for: "
            f"{', '.join(missing)}. Apply postgres/init/03_add_updated_at.sql "
            "as the PostgreSQL owner, then rerun ingestion."
        )


def _read_checkpoint(sf_connection: Any, settings: Settings, table: str) -> datetime | None:
    cursor = sf_connection.cursor()
    try:
        cursor.execute(
            f"SELECT successful_cutoff FROM {_qualified(settings, CHECKPOINT_TABLE)} "
            "WHERE source_table = %s",
            (table,),
        )
        row = cursor.fetchone()
        return row[0] if row else None
    finally:
        cursor.close()


def _capture_upper_bound(pg_connection: Any) -> datetime:
    with pg_connection.cursor() as cursor:
        cursor.execute("SELECT clock_timestamp()")
        return cursor.fetchone()[0]


def _create_stage(sf_connection: Any, settings: Settings, spec: TableSpec) -> str:
    stage = f"TMP_CDC_{spec.name.upper()}"
    cursor = sf_connection.cursor()
    try:
        cursor.execute(
            f"CREATE OR REPLACE TEMPORARY TABLE {stage} "
            f"LIKE {_qualified(settings, spec.name)}"
        )
    finally:
        cursor.close()
    return stage


def _extract_to_stage(
    pg_connection: Any,
    sf_connection: Any,
    settings: Settings,
    spec: TableSpec,
    stage: str,
    lower_bound: datetime | None,
    upper_bound: datetime,
) -> int:
    query = postgres_extract_sql(spec, settings.pg_schema, lower_bound is None)
    parameters: dict[str, datetime] = {"upper_bound": upper_bound}
    if lower_bound is not None:
        parameters["lower_bound"] = lower_bound

    columns = ", ".join(spec.all_columns)
    placeholders = ", ".join(["%s"] * len(spec.all_columns))
    insert_sql = f"INSERT INTO {stage} ({columns}) VALUES ({placeholders})"
    extracted = 0

    # A named cursor streams from PostgreSQL instead of materializing the result.
    cursor_name = f"cdc_{spec.name}"
    with pg_connection.cursor(name=cursor_name) as pg_cursor:
        pg_cursor.execute(query, parameters)
        while batch := pg_cursor.fetchmany(settings.batch_size):
            sf_cursor = sf_connection.cursor()
            try:
                sf_cursor.executemany(insert_sql, batch)
            finally:
                sf_cursor.close()
            extracted += len(batch)
    return extracted


def _merge_counts(cursor: Any) -> tuple[int, int]:
    row = cursor.fetchone()
    if row is None:
        return 0, 0
    labels = [description[0].lower() for description in cursor.description]
    values = dict(zip(labels, row, strict=True))
    inserted = int(values.get("number of rows inserted", 0) or 0)
    updated = int(values.get("number of rows updated", 0) or 0)
    return inserted, updated


def _merge_table(
    sf_connection: Any,
    settings: Settings,
    spec: TableSpec,
    stage: str,
    upper_bound: datetime,
    extracted: int,
) -> tuple[int, int]:
    cursor = sf_connection.cursor()
    transaction_started = False
    try:
        cursor.execute("BEGIN")
        transaction_started = True
        cursor.execute(
            snowflake_merge_sql(spec, _qualified(settings, spec.name), stage)
        )
        inserted, updated = _merge_counts(cursor)
        cursor.execute(
            checkpoint_merge_sql(_qualified(settings, CHECKPOINT_TABLE)),
            (spec.name, upper_bound, extracted, inserted, updated),
        )
        cursor.execute("COMMIT")
        transaction_started = False
        return inserted, updated
    except Exception:
        if transaction_started:
            try:
                cursor.execute("ROLLBACK")
            except Exception:
                LOGGER.exception("Snowflake rollback failed for %s", spec.name)
        raise
    finally:
        cursor.close()


def _process_table(
    pg_connection: Any,
    sf_connection: Any,
    settings: Settings,
    spec: TableSpec,
) -> TableResult:
    previous_cutoff = _read_checkpoint(sf_connection, settings, spec.name)
    upper_bound = _capture_upper_bound(pg_connection)
    lower_bound = (
        previous_cutoff - timedelta(minutes=settings.overlap_minutes)
        if previous_cutoff is not None
        else None
    )

    LOGGER.info(
        "Extracting %s with window (%s, %s]",
        spec.name,
        lower_bound if lower_bound is not None else "first run",
        upper_bound,
    )
    # DDL and staging loads deliberately happen before the atomic DML transaction.
    stage = _create_stage(sf_connection, settings, spec)
    extracted = _extract_to_stage(
        pg_connection,
        sf_connection,
        settings,
        spec,
        stage,
        lower_bound,
        upper_bound,
    )
    pg_connection.commit()
    inserted, updated = _merge_table(
        sf_connection, settings, spec, stage, upper_bound, extracted
    )
    result = TableResult(spec.name, upper_bound, extracted, inserted, updated)
    LOGGER.info(
        "Completed %s: extracted=%d inserted=%d updated=%d cutoff=%s",
        spec.name,
        extracted,
        inserted,
        updated,
        upper_bound,
    )
    return result


def run_ingestion(
    table_names: list[str] | tuple[str, ...] | None = None,
    *,
    settings: Settings | None = None,
) -> list[TableResult]:
    """Synchronize selected source tables and return per-table run metrics."""
    settings = settings or Settings.from_env()
    specs = select_tables(table_names)
    logging.basicConfig(
        level=getattr(logging, settings.log_level),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    pg_connection = None
    sf_connection = None
    lock_acquired = False
    try:
        pg_connection = psycopg.connect(**settings.postgres_connect_kwargs())
        with pg_connection.cursor() as cursor:
            cursor.execute(
                "SELECT pg_try_advisory_lock(%s)", (settings.advisory_lock_id,)
            )
            lock_acquired = bool(cursor.fetchone()[0])
        pg_connection.commit()
        if not lock_acquired:
            raise IngestionAlreadyRunningError(
                "Another ingestion command holds the PostgreSQL advisory lock"
            )

        _validate_postgres_schema(pg_connection, settings, specs)
        pg_connection.commit()

        sf_connection = snowflake.connector.connect(
            **settings.snowflake_connect_kwargs()
        )
        _setup_snowflake(sf_connection, settings, specs)
        return [
            _process_table(pg_connection, sf_connection, settings, spec)
            for spec in specs
        ]
    finally:
        if pg_connection is not None:
            if lock_acquired:
                try:
                    # A failed PostgreSQL statement leaves the transaction aborted.
                    # Roll it back before issuing the session-level unlock query.
                    pg_connection.rollback()
                    with pg_connection.cursor() as cursor:
                        cursor.execute(
                            "SELECT pg_advisory_unlock(%s)",
                            (settings.advisory_lock_id,),
                        )
                    pg_connection.commit()
                except Exception:
                    LOGGER.exception("Failed to release PostgreSQL advisory lock")
            pg_connection.close()
        if sf_connection is not None:
            sf_connection.close()
