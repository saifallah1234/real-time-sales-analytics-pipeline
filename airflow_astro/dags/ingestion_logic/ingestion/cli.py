import argparse
import logging

from .pipeline import run_ingestion
from .tables import TABLES


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Incrementally merge PostgreSQL rows into Snowflake RAW_DATA."
    )
    parser.add_argument(
        "--table",
        action="append",
        choices=tuple(TABLES),
        dest="tables",
        help="Ingest only this table; repeat for multiple tables.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    try:
        results = run_ingestion(args.tables)
    except Exception as exc:
        logging.getLogger(__name__).error("Ingestion failed: %s", exc)
        raise SystemExit(1) from exc

    for result in results:
        print(
            f"{result.table}: extracted={result.extracted_row_count}, "
            f"inserted={result.inserted_row_count}, "
            f"updated={result.updated_row_count}, "
            f"cutoff={result.successful_cutoff.isoformat()}"
        )
