"""Incremental PostgreSQL to Snowflake ingestion."""

from ingestion.pipeline import TableResult, run_ingestion

__all__ = ["TableResult", "run_ingestion"]
