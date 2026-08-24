"""Command-line interface for TuAlpha."""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Sequence
from pathlib import Path

from . import __version__
from .config import DEFAULT_BUNDLE_ROOT
from .exceptions import TualphaError
from .updater import DataUpdater, UpdateOptions, token_from_stdin


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="tualpha", description="A-share stock and ETF backtesting tools"
    )
    parser.add_argument("--version", action="version", version=f"tualpha {__version__}")
    commands = parser.add_subparsers(dest="command")
    update = commands.add_parser(
        "update",
        help="incrementally download Tushare CSV data and replace the current bundle",
    )
    update.add_argument(
        "--csv-dir",
        type=Path,
        required=True,
        help="explicit directory used to retain raw Tushare CSV files",
    )
    update.add_argument(
        "--bundle-root",
        type=Path,
        default=DEFAULT_BUNDLE_ROOT,
        help="bundle root (default: ~/.tualpha)",
    )
    update.add_argument("--bundle-name", default="tualpha")
    update.add_argument("--from", dest="start", metavar="YYYYMMDD")
    update.add_argument("--to", dest="end", metavar="YYYYMMDD")
    update.add_argument("--repair-from", metavar="YYYYMMDD")
    update.add_argument("--lookback", type=int, default=10)
    update.add_argument("--retries", type=int, default=3)
    update.add_argument("--backoff", type=float, default=2.0)
    update.add_argument("--token-stdin", action="store_true")
    update.add_argument(
        "--index-weight",
        action="append",
        default=[],
        metavar="INDEX_CODE",
        help="add an index beyond the five default PIT constituent datasets",
    )
    update.add_argument("--dry-run", action="store_true")
    update.add_argument("--show-progress", action="store_true")
    update.add_argument("--json", action="store_true", dest="json_output")
    return parser


def run_cli(argv: Sequence[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    if args.command is None:
        parser.print_help()
        return 0
    if args.command != "update":  # pragma: no cover - argparse constrains this
        parser.error(f"unknown command: {args.command}")

    token = token_from_stdin() if args.token_stdin else os.environ.get("TUSHARE_TOKEN")
    if not token:
        print(
            "error: TUSHARE_TOKEN is not set; update aborted",
            file=sys.stderr,
        )
        return 2
    try:
        options = UpdateOptions(
            csv_dir=args.csv_dir,
            bundle_root=args.bundle_root,
            bundle_name=args.bundle_name,
            start=args.start,
            end=args.end,
            repair_from=args.repair_from,
            lookback=args.lookback,
            retries=args.retries,
            backoff=args.backoff,
            dry_run=args.dry_run,
            show_progress=args.show_progress,
            index_weight_codes=tuple(args.index_weight),
        )
        result = DataUpdater(options, token=token).run()
    except (TualphaError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    payload = {
        "run_id": result.run_id,
        "updated_dates": list(result.updated_dates),
        "updated_files": result.updated_files,
        "bundle_path": result.bundle_path,
        "dry_run": result.dry_run,
    }
    if args.json_output:
        print(json.dumps(payload, ensure_ascii=False))
    else:
        print(f"updated dates: {len(result.updated_dates)}")
        print(f"updated files: {result.updated_files}")
        print(f"bundle: {result.bundle_path or 'not published (dry-run)'}")
    return 0


def main() -> None:
    raise SystemExit(run_cli())
