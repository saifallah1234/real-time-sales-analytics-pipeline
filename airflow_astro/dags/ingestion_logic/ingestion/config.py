import os
import re
from dataclasses import dataclass

from dotenv import load_dotenv


_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")


def _required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def _positive_int(name: str, default: int, *, allow_zero: bool = False) -> int:
    raw = os.getenv(name, str(default))
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from exc
    minimum = 0 if allow_zero else 1
    if value < minimum:
        raise ValueError(f"{name} must be at least {minimum}")
    return value


def _identifier(name: str, default: str) -> str:
    value = os.getenv(name, default)
    if not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"{name} is not a valid unquoted SQL identifier: {value!r}")
    return value


@dataclass(frozen=True)
class Settings:
    pg_host: str
    pg_port: int
    pg_database: str
    pg_user: str
    pg_password: str
    pg_schema: str
    snowflake_account: str
    snowflake_user: str
    snowflake_password: str
    snowflake_warehouse: str
    snowflake_role: str | None
    snowflake_database: str
    snowflake_schema: str
    batch_size: int
    overlap_minutes: int
    advisory_lock_id: int
    log_level: str

    @classmethod
    def from_env(cls) -> "Settings":
        load_dotenv()
        log_level = os.getenv("INGESTION_LOG_LEVEL", "INFO").upper()
        if log_level not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
            raise ValueError(f"Invalid INGESTION_LOG_LEVEL: {log_level!r}")

        return cls(
            pg_host=os.getenv("PG_HOST", "localhost"),
            pg_port=_positive_int("PG_PORT", 5432),
            pg_database=os.getenv("PG_DATABASE", "cdc_project"),
            pg_user=os.getenv("PG_USER", "saif"),
            pg_password=_required("PG_PASSWORD"),
            pg_schema=_identifier("PG_SCHEMA", "cdc_schema"),
            snowflake_account=_required("SNOWFLAKE_ACCOUNT"),
            snowflake_user=_required("SNOWFLAKE_USER"),
            snowflake_password=_required("SNOWFLAKE_PASSWORD"),
            snowflake_warehouse=_required("SNOWFLAKE_WAREHOUSE"),
            snowflake_role=os.getenv("SNOWFLAKE_ROLE") or None,
            snowflake_database=_identifier("SNOWFLAKE_DATABASE", "CDC_DATABASE"),
            snowflake_schema=_identifier("SNOWFLAKE_SCHEMA", "RAW_DATA"),
            batch_size=_positive_int("INGESTION_BATCH_SIZE", 1000),
            overlap_minutes=_positive_int(
                "INGESTION_OVERLAP_MINUTES", 5, allow_zero=True
            ),
            advisory_lock_id=_positive_int("INGESTION_ADVISORY_LOCK_ID", 723303724),
            log_level=log_level,
        )

    def postgres_connect_kwargs(self) -> dict[str, object]:
        return {
            "host": self.pg_host,
            "port": self.pg_port,
            "dbname": self.pg_database,
            "user": self.pg_user,
            "password": self.pg_password,
        }

    def snowflake_connect_kwargs(self) -> dict[str, object]:
        values: dict[str, object] = {
            "account": self.snowflake_account,
            "user": self.snowflake_user,
            "password": self.snowflake_password,
            "warehouse": self.snowflake_warehouse,
            "database": self.snowflake_database,
            "schema": self.snowflake_schema,
            "application": "cdc_project_ingestion",
        }
        if self.snowflake_role:
            values["role"] = self.snowflake_role
        return values
