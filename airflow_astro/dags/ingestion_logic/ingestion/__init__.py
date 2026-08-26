"""Incremental PostgreSQL to Snowflake ingestion."""

from .pipeline import TableResult, run_ingestion

__all__ = ["TableResult", "run_ingestion"]
