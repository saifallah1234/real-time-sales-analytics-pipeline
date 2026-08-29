"""Command-line entry point for the customer CDC load test."""

from __future__ import annotations

import argparse
from pathlib import Path

from .cdc_load_test import LoadTestSettings, print_summary, run_load_test, write_report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the 1,000-customer PostgreSQL-to-Snowflake CDC load test."
    )
    parser.add_argument(
        "--report",
        type=Path,
        help="JSON report path (default: reports/cdc_load_test_<run-id>.json)",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    settings = LoadTestSettings.from_env()
    report = run_load_test(settings)
    report_path = args.report or settings.report_directory / (
        f"cdc_load_test_{report['run_id']}.json"
    )
    write_report(report, report_path)
    print_summary(report)
    print(f"Report: {report_path.resolve()}")
    raise SystemExit(0 if report["status"] == "PASSED" else 1)


if __name__ == "__main__":
    main()
