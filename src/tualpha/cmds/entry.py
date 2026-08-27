"""Command-line interface for TuAlpha data and backtesting tools."""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Sequence
from pathlib import Path

from .. import __version__
from ..data.bundle.compactor import compact_bundle
from ..data.bundle.parquet_schema import TABLE_SPECS
from ..data.bundle.updater import DataUpdater, UpdateOptions, token_from_stdin
from ..data.quality import QualityReporter, QualityRunner, format_summary
from ..foundation.config import DEFAULT_BUNDLE_ROOT
from ..foundation.exceptions import TualphaError


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="tualpha", description="A-share stock and ETF backtesting tools"
    )
    parser.add_argument("--version", action="version", version=f"tualpha {__version__}")
    commands = parser.add_subparsers(dest="command")

    build = commands.add_parser(
        "build", help="build a complete Parquet Bundle from Tushare"
    )
    build.add_argument("--bundle-root", type=Path, default=DEFAULT_BUNDLE_ROOT)
    build.add_argument("--bundle-name", default="tualpha")
    build.add_argument("--from", dest="start", metavar="YYYYMMDD", required=True)
    build.add_argument("--to", dest="end", metavar="YYYYMMDD")
    build.add_argument("--retries", type=int, default=3)
    build.add_argument("--backoff", type=float, default=2.0)
    build.add_argument("--token-stdin", action="store_true")
    build.add_argument(
        "--index-weight", action="append", default=[], metavar="INDEX_CODE"
    )
    build.add_argument(
        "--index-daily", action="append", default=[], metavar="INDEX_CODE"
    )
    build.add_argument("--dry-run", action="store_true")
    build_progress = build.add_mutually_exclusive_group()
    build_progress.add_argument(
        "--show-progress", dest="show_progress", action="store_true"
    )
    build_progress.add_argument(
        "--no-progress", dest="show_progress", action="store_false"
    )
    build.set_defaults(show_progress=True)
    build.add_argument("--json", action="store_true", dest="json_output")

    update = commands.add_parser(
        "update", help="download Tushare data into a new Parquet generation"
    )
    update.add_argument("--bundle-root", type=Path, default=DEFAULT_BUNDLE_ROOT)
    update.add_argument("--bundle-name", default="tualpha")
    update.add_argument("--from", dest="start", metavar="YYYYMMDD")
    update.add_argument("--to", dest="end", metavar="YYYYMMDD")
    update.add_argument("--repair-from", metavar="YYYYMMDD")
    update.add_argument(
        "--compact",
        action="store_true",
        help="accepted for compatibility; yearly Parquet needs no compaction",
    )
    update.add_argument(
        "--lookback",
        type=int,
        default=0,
        help="explicitly refresh the latest N open sessions (default: missing data only)",
    )
    update.add_argument("--retries", type=int, default=3)
    update.add_argument("--backoff", type=float, default=2.0)
    update.add_argument("--token-stdin", action="store_true")
    update.add_argument(
        "--index-weight", action="append", default=[], metavar="INDEX_CODE"
    )
    update.add_argument(
        "--index-daily", action="append", default=[], metavar="INDEX_CODE"
    )
    update.add_argument("--dry-run", action="store_true")
    progress = update.add_mutually_exclusive_group()
    progress.add_argument("--show-progress", dest="show_progress", action="store_true")
    progress.add_argument("--no-progress", dest="show_progress", action="store_false")
    update.set_defaults(show_progress=True)
    update.add_argument("--json", action="store_true", dest="json_output")

    quality = commands.add_parser(
        "quality", help="run local table-level data quality checks"
    )
    quality.add_argument("--bundle-root", type=Path, default=DEFAULT_BUNDLE_ROOT)
    quality.add_argument("--bundle-name", default="tualpha")
    quality.add_argument("--table", action="append", choices=sorted(TABLE_SPECS))
    quality.add_argument("--full-hash", action="store_true")
    quality.add_argument("--report-dir", type=Path)
    quality.add_argument("--json", action="store_true", dest="json_output")

    compact = commands.add_parser(
        "compact",
        help="validate the active yearly Parquet Bundle (compatibility command)",
    )
    compact.add_argument("--bundle-root", type=Path, default=DEFAULT_BUNDLE_ROOT)
    compact.add_argument("--bundle-name", default="tualpha")
    compact.add_argument("--json", action="store_true", dest="json_output")
    return parser


def _run_quality(args: argparse.Namespace) -> int:
    try:
        report = QualityRunner(args.bundle_root, args.bundle_name).run(
            tuple(args.table) if args.table else None,
            full_hash=args.full_hash,
        )
        report_root = (
            args.report_dir
            or Path(args.bundle_root).expanduser() / "reports" / "quality"
        )
        QualityReporter(report_root).write(report)
    except (TualphaError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    if args.json_output:
        print(
            json.dumps(
                {
                    "generation": report.generation,
                    "fail": report.fail_count,
                    "warn": report.warn_count,
                    "report": str(report.output_dir),
                },
                ensure_ascii=False,
            )
        )
    else:
        print(format_summary(report))
    return 1 if report.fail_count else 0


def run_cli(argv: Sequence[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    if args.command is None:
        parser.print_help()
        return 0
    if args.command == "quality":
        return _run_quality(args)
    if args.command == "compact":
        try:
            result, manifest = compact_bundle(args.bundle_root, args.bundle_name)
        except (TualphaError, ValueError) as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 1
        payload = {
            "generation": manifest["generation"],
            "bundle_path": str(result.path),
            "asset_count": result.asset_count,
            "session_count": result.session_count,
        }
        print(
            json.dumps(payload, ensure_ascii=False)
            if args.json_output
            else f"bundle valid: {result.path}"
        )
        return 0
    if args.command not in {"build", "update"}:
        parser.error(f"unknown command: {args.command}")

    token = token_from_stdin() if args.token_stdin else os.environ.get("TUSHARE_TOKEN")
    if not token:
        print(
            f"error: TUSHARE_TOKEN is not set; {args.command} aborted",
            file=sys.stderr,
        )
        return 2
    try:
        is_build = args.command == "build"
        options = UpdateOptions(
            bundle_root=args.bundle_root,
            bundle_name=args.bundle_name,
            start=args.start,
            end=args.end,
            repair_from=None if is_build else args.repair_from,
            lookback=0 if is_build else args.lookback,
            retries=args.retries,
            backoff=args.backoff,
            dry_run=args.dry_run,
            full=is_build,
            compact=False if is_build else args.compact,
            show_progress=args.show_progress and not args.json_output,
            index_daily_codes=tuple(args.index_daily),
            index_weight_codes=tuple(args.index_weight),
        )
        result = DataUpdater(options, token=token).run()
    except (TualphaError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    payload = {
        "operation": args.command,
        "run_id": result.run_id,
        "updated_dates": list(result.updated_dates),
        "updated_files": result.updated_files,
        "bundle_path": result.bundle_path,
        "dry_run": result.dry_run,
    }
    if args.json_output:
        print(json.dumps(payload, ensure_ascii=False))
    else:
        label = "built" if args.command == "build" else "updated"
        print(f"{label} dates: {len(result.updated_dates)}")
        print(f"bundle files: {result.updated_files}")
        print(f"bundle: {result.bundle_path or 'not published (dry-run)'}")
    return 0


def main() -> None:
    raise SystemExit(run_cli())
