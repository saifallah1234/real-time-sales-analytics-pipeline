"""Command-line entry point for the pipeline health-check collector."""

from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime

from .monitoring import OK, MonitorSettings, run_monitoring, summarize


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Record CDC pipeline health checks in PostgreSQL for Grafana."
    )
    parser.add_argument(
        "--interval",
        type=float,
        metavar="SECONDS",
        help="Keep collecting every SECONDS until interrupted (default: run once).",
    )
    return parser


def _run_once(settings: MonitorSettings) -> None:
    run_id, results = run_monitoring(settings)
    counts = summarize(results)
    print(
        f"{datetime.now():%H:%M:%S} run {run_id}: "
        f"ok={counts['ok']} warn={counts['warn']} fail={counts['fail']}"
    )
    for result in results:
        if result.status != OK:
            print(f"  {result.status.upper():4} {result.check_name} {result.subject}: {result.detail}")


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    if args.interval is not None and args.interval <= 0:
        parser.error("--interval must be greater than zero")

    try:
        settings = MonitorSettings.from_env()
        _run_once(settings)
    except Exception as exc:
        print(f"Monitoring run failed: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc

    if args.interval is None:
        return
    try:
        while True:
            time.sleep(args.interval)
            try:
                _run_once(settings)
            except Exception as exc:
                print(f"Monitoring run failed: {exc}", file=sys.stderr)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
