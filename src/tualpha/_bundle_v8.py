"""Schema-7 HDF5 Bundle build, validation, and atomic publication."""

from __future__ import annotations

import gc
import json
import os
import shutil
import time
from collections.abc import Sequence
from contextlib import ExitStack
from datetime import datetime
from pathlib import Path
from typing import Any, Self
from uuid import uuid4
from zoneinfo import ZoneInfo

import h5py
import numpy as np
import pandas as pd
from filelock import FileLock

from . import _bundle_core as core
from ._calendar_store import (
    CALENDAR_EXCHANGE,
    CALENDAR_SOURCE,
    SessionCalendar,
    normalize_trade_calendar,
    sessions_from_trade_calendar,
)
from ._csv_hdf5 import (
    DAILY_ROLES,
    bucket_daily,
    bucket_daily_role,
    bucket_finance,
    write_daily,
    write_daily_role,
    write_finance_table,
    write_index_weights,
)
from ._hdf5_store import (
    ASSETS_FILE,
    BUNDLE_PROTOCOL,
    BUNDLE_SCHEMA_VERSION,
    HDF5_FILES,
    REQUIRED_BUNDLE_FILES,
    TRADE_DATES_FILE,
    date_to_int,
    dates_to_int,
    initialize_h5,
    load_assets_manifest,
    open_h5_checked,
    sha256_file,
    validate_bundle_directory,
    write_assets_manifest,
)
from .config import DEFAULT_BUNDLE_ROOT
from .exceptions import DataError
from .tushare_fields import FINANCIAL_FIELDS

BUNDLE_NAME = "tualpha"
UPDATE_STATUS_SCHEMA_VERSION = 3
_FINANCIAL_TABLES = ("balancesheet", "income", "cashflow", "fina_indicator")


def validate_bundle_name(bundle_name: str) -> str:
    return core.validate_bundle_name(bundle_name)


def bundle_parent(bundle_root: str | Path = DEFAULT_BUNDLE_ROOT) -> Path:
    return Path(bundle_root).expanduser()


def bundle_path(
    bundle_root: str | Path = DEFAULT_BUNDLE_ROOT,
    bundle_name: str = BUNDLE_NAME,
) -> Path:
    validate_bundle_name(bundle_name)
    return bundle_parent(bundle_root) / "bundle"


def bundle_lock_path(
    bundle_root: str | Path = DEFAULT_BUNDLE_ROOT,
    bundle_name: str = BUNDLE_NAME,
) -> Path:
    return core.bundle_lock_path(bundle_root, bundle_name)


def update_status_path(bundle_root: str | Path = DEFAULT_BUNDLE_ROOT) -> Path:
    return core.update_status_path(bundle_root)


def latest_bundle_path(
    bundle_root: str | Path = DEFAULT_BUNDLE_ROOT,
    bundle_name: str = BUNDLE_NAME,
) -> Path:
    root = Path(bundle_root).expanduser()
    destination = bundle_path(root, bundle_name)
    _recover_interrupted_bundle(root)
    try:
        validate_bundle_directory(destination, full_hash=False)
    except DataError as exc:
        raise DataError(
            f"bundle does not exist or is incomplete; run `tualpha update`: "
            f"{destination}"
        ) from exc
    return destination


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    with temporary.open("w", encoding="utf-8") as stream:
        json.dump(payload, stream, ensure_ascii=False, indent=2)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def _date_value(value: object) -> int:
    if value is None or pd.isna(value):
        return 0
    return date_to_int(value)


def _asset_payload(record: Any, *, tradable: bool) -> dict[str, Any]:
    board = str(record.board)
    minimum = 200 if board == "star" else 100
    increment = 1 if board in {"star", "bse"} else 100
    return {
        "sid": int(record.sid),
        "ts_code": str(record.ts_code).upper(),
        "symbol": str(record.ts_code).split(".")[0],
        "name": str(record.name),
        "asset_type": str(record.asset_type),
        "tradable": bool(tradable),
        "exchange": str(record.exchange),
        "board": board,
        "list_date": _date_value(record.start_date),
        "delist_date": _date_value(record.end_date),
        "price_tick": float(record.price_tick),
        "round_lot": increment,
        "minimum_order": minimum,
        "settlement_days": 1 if tradable else 0,
    }


def _write_bundle_files(
    output_dir: Path,
    csv_dir: Path,
    staging_root: Path,
    daily_buckets: Any,
    records: Sequence[Any],
    benchmark_records: Sequence[Any],
    calendar: SessionCalendar,
    start_session: pd.Timestamp,
    end_session: pd.Timestamp,
    generated_at: str,
    generation: str,
    *,
    show_progress: bool,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=False)
    np.save(
        output_dir / TRADE_DATES_FILE,
        dates_to_int(calendar.sessions),
        allow_pickle=False,
    )
    handles: dict[str, h5py.File] = {}
    row_counts: dict[str, int] = {}
    index_codes: list[str] = []
    buckets_root = staging_root / "buckets"
    try:
        with ExitStack() as stack:
            for filename, role in HDF5_FILES.items():
                handles[role] = stack.enter_context(
                    initialize_h5(
                        output_dir / filename,
                        role=role,
                        generation=generation,
                        generated_at=generated_at,
                    )
                )
            row_counts["daily"] = write_daily(
                daily_buckets,
                handles["daily"],
                records,
                benchmark_records,
                calendar,
            )
            shutil.rmtree(daily_buckets.root, ignore_errors=True)

            for role in DAILY_ROLES:
                buckets, categories = bucket_daily_role(
                    csv_dir,
                    buckets_root,
                    role,
                    show_progress=show_progress,
                )
                row_counts[role.role] = write_daily_role(
                    buckets,
                    handles[role.role],
                    role,
                    categories,
                    records,
                    calendar,
                )
                shutil.rmtree(buckets.root, ignore_errors=True)

            finance = handles["finance"]
            finance_root = finance["data"]
            finance_fields: dict[str, list[str]] = {}
            allowed_codes = {str(record.ts_code) for record in records}
            finance_rows = 0
            for table in _FINANCIAL_TABLES:
                group = finance_root.create_group(table, track_order=True)
                buckets = bucket_finance(
                    csv_dir,
                    buckets_root,
                    table,
                    show_progress=show_progress,
                )
                finance_rows += write_finance_table(
                    buckets, group, table, allowed_codes
                )
                shutil.rmtree(buckets.root, ignore_errors=True)
                finance_fields[table] = [
                    field
                    for field in FINANCIAL_FIELDS[table]
                    if field
                    not in {
                        "ts_code",
                        "ann_date",
                        "f_ann_date",
                        "end_date",
                        "report_type",
                        "comp_type",
                        "end_type",
                        "update_flag",
                    }
                ]
            row_counts["finance"] = finance_rows
            finance.attrs["fields"] = json.dumps(finance_fields, ensure_ascii=False)
            finance.attrs["point_in_time"] = (
                "effective_ann_date < session and end_date <= session"
            )

            sid_by_code = {
                str(record.ts_code): int(record.sid)
                for record in [*records, *benchmark_records]
            }
            weight_rows, index_codes = write_index_weights(
                csv_dir,
                handles["index_weight"],
                sid_by_code,
                end_session,
            )
            row_counts["index_weight"] = weight_rows
            handles["index_weight"].attrs["point_in_time"] = "snapshot_date < session"
            handles["index_weight"].attrs["weight_unit"] = "percent"
            handles["daily"].attrs["price_domain"] = "raw"
            handles["daily"].attrs["volume_unit"] = "shares_or_provider_index_unit"
            handles["adj_factor"].attrs["adjustment"] = (
                "qfq=raw*factor(date)/factor(reference); hfq=raw*factor(date)"
            )
            for handle in handles.values():
                handle.flush()
    finally:
        shutil.rmtree(buckets_root, ignore_errors=True)

    files: dict[str, dict[str, Any]] = {}
    for filename in sorted(REQUIRED_BUNDLE_FILES.difference({ASSETS_FILE})):
        path = output_dir / filename
        role = HDF5_FILES.get(filename, "trade_dates")
        files[filename] = {
            "role": role,
            "size": path.stat().st_size,
            "sha256": sha256_file(path),
            "rows": int(row_counts.get(role, len(calendar.sessions))),
        }
    asset_rows = [
        *(_asset_payload(record, tradable=True) for record in records),
        *(_asset_payload(record, tradable=False) for record in benchmark_records),
    ]
    manifest = {
        "protocol": BUNDLE_PROTOCOL,
        "schema_version": BUNDLE_SCHEMA_VERSION,
        "generation": generation,
        "generated_at": generated_at,
        "start_session": date_to_int(start_session),
        "end_session": date_to_int(end_session),
        "session_count": len(calendar.sessions),
        "asset_count": len(records),
        "index_count": len(benchmark_records),
        "calendar_source": f"{CALENDAR_SOURCE}:{CALENDAR_EXCHANGE}",
        "build_pipeline": "csv->staging-hash-buckets->sorted-hdf5",
        "files": files,
        "assets": asset_rows,
        "index_codes": index_codes,
        "pit_rules": {
            "finance": "effective_ann_date < session and end_date <= session",
            "index_weight": "snapshot_date < session",
        },
    }
    write_assets_manifest(output_dir / ASSETS_FILE, manifest)
    return manifest


def _iter_compound_datasets(group: h5py.Group):
    for value in group.values():
        if isinstance(value, h5py.Dataset):
            yield value
        elif isinstance(value, h5py.Group):
            yield from _iter_compound_datasets(value)


def validate_hdf5_bundle(
    path: str | Path,
    *,
    full_hash: bool = True,
    full_scan: bool = True,
) -> dict[str, Any]:
    root = Path(path)
    manifest = validate_bundle_directory(root, full_hash=full_hash)
    generation = str(manifest["generation"])
    valid_dates = set(np.load(root / TRADE_DATES_FILE, allow_pickle=False).tolist())
    for filename, role in HDF5_FILES.items():
        handle = open_h5_checked(
            root / filename,
            expected_role=role,
            expected_generation=generation,
        )
        try:
            if not full_scan:
                continue
            for dataset in _iter_compound_datasets(handle["data"]):
                if dataset.dtype.names is None:
                    raise DataError(f"non-compound HDF5 dataset: {dataset.name}")
                date_field = (
                    "trade_date"
                    if "trade_date" in dataset.dtype.names
                    else "snapshot_date"
                    if "snapshot_date" in dataset.dtype.names
                    else "end_date"
                    if "end_date" in dataset.dtype.names
                    else None
                )
                if date_field is None:
                    raise DataError(f"HDF5 dataset has no date field: {dataset.name}")
                dates = np.asarray(dataset.fields(date_field)[:], dtype=np.int64)
                if role not in {"finance", "index_weight"}:
                    if len(dates) > 1 and np.any(dates[1:] <= dates[:-1]):
                        raise DataError(
                            f"HDF5 daily dates are not unique and sorted: "
                            f"{dataset.name}"
                        )
                    if any(int(date) not in valid_dates for date in dates):
                        raise DataError(
                            "HDF5 daily dates are outside trade_dates.npy: "
                            f"{dataset.name}"
                        )
                elif len(dates) > 1 and np.any(dates[1:] < dates[:-1]):
                    raise DataError(f"HDF5 dates are not sorted: {dataset.name}")
        finally:
            handle.close()
    return manifest


class LockedBundleData:
    """Read-locked collection of schema-7 Bundle components."""

    def __init__(
        self,
        bundle_root: str | Path,
        bundle_name: str = BUNDLE_NAME,
    ) -> None:
        self._lock_key, _ = core.acquire_bundle_read_lock(bundle_root, bundle_name)
        try:
            self.path = latest_bundle_path(bundle_root, bundle_name)
            self.manifest = load_assets_manifest(self.path / ASSETS_FILE)
            from .assets import AssetFinder
            from .calendar import ChinaTradingCalendar

            self.asset_finder = AssetFinder(bundle_root, bundle_name)
            self.calendar = ChinaTradingCalendar(bundle_root, bundle_name)
            self.h5 = {
                role: open_h5_checked(
                    self.path / filename,
                    expected_role=role,
                    expected_generation=str(self.manifest["generation"]),
                )
                for filename, role in HDF5_FILES.items()
            }
        except Exception:
            core.release_bundle_read_lock(self._lock_key)
            self._lock_key = None
            raise

    def close(self) -> None:
        for handle in getattr(self, "h5", {}).values():
            handle.close()
        self.h5 = {}
        if getattr(self, "_lock_key", None) is not None:
            core.release_bundle_read_lock(self._lock_key)
            self._lock_key = None

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()


def load_bundle_data(
    bundle_root: str | Path = DEFAULT_BUNDLE_ROOT,
    bundle_name: str = BUNDLE_NAME,
) -> LockedBundleData:
    return LockedBundleData(bundle_root, bundle_name)


def _replace_directory(source: Path, destination: Path) -> None:
    for attempt in range(12):
        try:
            os.replace(source, destination)
            return
        except PermissionError:
            if os.name != "nt" or attempt == 11:
                raise
            gc.collect()
            time.sleep(0.05 * (attempt + 1))


def _recover_interrupted_bundle(root: Path) -> None:
    destination = root / "bundle"
    if destination.is_dir():
        return
    rollback_root = root / ".rollback"
    if not rollback_root.is_dir():
        return
    candidates = sorted(
        (path / "bundle" for path in rollback_root.iterdir()),
        key=lambda path: path.stat().st_mtime_ns if path.exists() else -1,
        reverse=True,
    )
    for candidate in candidates:
        if not candidate.is_dir():
            continue
        try:
            validate_bundle_directory(candidate, full_hash=False)
        except DataError:
            continue
        _replace_directory(candidate, destination)
        break


def _publish_fixed_bundle(staged: Path, destination: Path) -> None:
    root = destination.parent
    rollback = root / ".rollback" / uuid4().hex / "bundle"
    rollback.parent.mkdir(parents=True, exist_ok=False)
    moved_previous = False
    try:
        if destination.exists():
            _replace_directory(destination, rollback)
            moved_previous = True
        _replace_directory(staged, destination)
        validate_bundle_directory(destination, full_hash=False)
    except Exception:
        if destination.exists():
            shutil.rmtree(destination, ignore_errors=True)
        if moved_previous and rollback.exists():
            _replace_directory(rollback, destination)
        raise
    else:
        shutil.rmtree(rollback.parent, ignore_errors=True)


def cleanup_legacy_storage(bundle_root: str | Path) -> tuple[str, ...]:
    """Remove obsolete Bcolz/SQLite/DuckDB generations after schema-7 publish."""

    root = Path(bundle_root).expanduser().resolve()
    removed: list[str] = []
    for name in ("bundles", "cache"):
        target = root / name
        if not target.exists():
            continue
        if target.is_symlink() or target.resolve().parent != root:
            raise DataError(f"refusing to remove unsafe legacy path: {target}")
        shutil.rmtree(target)
        removed.append(str(target))
    for pattern in ("*.bcolz", "*.duckdb"):
        for target in root.rglob(pattern):
            if target.is_symlink() or root not in target.resolve().parents:
                raise DataError(f"refusing to remove unsafe legacy path: {target}")
            if target.is_dir():
                shutil.rmtree(target)
            else:
                target.unlink()
            removed.append(str(target))
    return tuple(removed)


def _record_bundle_build(
    root: Path,
    *,
    bundle_name: str,
    csv_dir: Path,
    started_at: str,
    result: Any,
    manifest: dict[str, Any],
    removed_legacy_paths: Sequence[str],
) -> None:
    status_path = update_status_path(root)
    previous: dict[str, Any] = {}
    if status_path.is_file():
        try:
            previous = json.loads(status_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            previous = {}
    completed_at = datetime.now(ZoneInfo("UTC")).isoformat()
    build = {
        "started_at": started_at,
        "completed_at": completed_at,
        "bundle_path": str(result.path),
        "generation": manifest["generation"],
        "start_session": result.start_session.strftime("%Y-%m-%d"),
        "end_session": result.end_session.strftime("%Y-%m-%d"),
        "asset_count": result.asset_count,
        "session_count": result.session_count,
        "storage": "hdf5",
        "bundle_schema": BUNDLE_SCHEMA_VERSION,
        "build_pipeline": manifest["build_pipeline"],
    }
    payload = {
        "schema_version": UPDATE_STATUS_SCHEMA_VERSION,
        "operation": "bundle_build",
        "status": "succeeded",
        "phase": "reopen",
        "bundle_name": bundle_name,
        "csv_dir": str(csv_dir.resolve()),
        "bundle_root": str(root.resolve()),
        "active_generation": manifest["generation"],
        "last_bundle_build": build,
        "verification": {
            "structural": "passed",
            "sha256": "passed",
            "reader": "passed",
        },
        "legacy_cleanup": {
            "status": "passed",
            "removed_paths": list(removed_legacy_paths),
        },
    }
    if "last_success" in previous:
        payload["last_success"] = previous["last_success"]
    _write_json_atomic(status_path, payload)


def _cleanup_stale_staging(root: Path) -> None:
    staging = root / ".staging"
    if not staging.is_dir():
        return
    for candidate in staging.glob("bundle-*"):
        if candidate.is_dir():
            shutil.rmtree(candidate, ignore_errors=True)


def build_bundle(
    csv_dir: str | Path,
    *,
    bundle_root: str | Path = DEFAULT_BUNDLE_ROOT,
    bundle_name: str = BUNDLE_NAME,
    rebuild_normalized: bool = False,
    show_progress: bool = False,
    record_status: bool = True,
    _maintenance_locked: bool = False,
) -> Any:
    """Build via CSV hash buckets and atomically publish ``bundle/``."""

    del rebuild_normalized
    source = Path(csv_dir).expanduser().resolve()
    root = Path(bundle_root).expanduser().resolve()
    validate_bundle_name(bundle_name)
    if core.paths_overlap(source, root):
        raise DataError(
            "CSV directory and Bundle root must not overlap, be equal, or "
            "contain one another"
        )
    for filename in ("stock_basic.csv", "etf_basic.csv", "trade_cal.csv"):
        if not (source / filename).is_file():
            raise DataError(
                f"required raw data file does not exist: {source / filename}"
            )
    started_at = datetime.now(ZoneInfo("UTC")).isoformat()
    update_lock = FileLock(str(root / ".locks" / "update.lock"), thread_local=False)
    context = ExitStack()
    if not _maintenance_locked:
        (root / ".locks").mkdir(parents=True, exist_ok=True)
        context.enter_context(update_lock)
    with context:
        if not _maintenance_locked:
            core.reject_interrupted_data_update(root)
        _recover_interrupted_bundle(root)
        _cleanup_stale_staging(root)
        generation = str(uuid4())
        generated_at = datetime.now(ZoneInfo("UTC")).isoformat()
        staging_root = root / ".staging" / f"bundle-{generation}"
        staged_bundle = staging_root / "bundle"
        destination = bundle_path(root, bundle_name)
        try:
            staging_root.mkdir(parents=True, exist_ok=False)
            daily_buckets, observations = bucket_daily(
                source,
                staging_root / "buckets",
                show_progress=show_progress,
            )
            tradable_dates = [
                bounds
                for (code, asset_type), bounds in observations.items()
                if asset_type in {"stock", "etf"}
            ]
            if not tradable_dates:
                raise DataError("CSV sources contain no stock or ETF daily data")
            start_session = min(bounds[0] for bounds in tradable_dates)
            end_session = max(bounds[1] for bounds in tradable_dates)
            trade_calendar = normalize_trade_calendar(
                pd.read_csv(
                    source / "trade_cal.csv",
                    dtype=str,
                    keep_default_na=False,
                )
            )
            bundle_sessions = sessions_from_trade_calendar(
                trade_calendar, start_session, end_session
            )
            calendar = SessionCalendar(bundle_sessions)
            sid_registry = core.SidRegistry(root)
            records = core.load_bundle_assets(
                source, observations, sid_registry, end_session
            )
            benchmark_records = core.load_benchmark_assets(
                observations,
                sid_registry,
                start_session,
                end_session,
            )
            manifest = _write_bundle_files(
                staged_bundle,
                source,
                staging_root,
                daily_buckets,
                records,
                benchmark_records,
                calendar,
                start_session,
                end_session,
                generated_at,
                generation,
                show_progress=show_progress,
            )
            if {path.name for path in staged_bundle.iterdir()} != REQUIRED_BUNDLE_FILES:
                raise DataError("staged Bundle does not contain exactly 12 files")
            validate_hdf5_bundle(staged_bundle, full_hash=True, full_scan=True)
            lock_key, _ = core.acquire_bundle_read_lock(root, bundle_name)
            try:
                _publish_fixed_bundle(staged_bundle, destination)
            finally:
                core.release_bundle_read_lock(lock_key)
            with load_bundle_data(root, bundle_name) as loaded:
                if loaded.manifest["generation"] != generation:
                    raise DataError("published Bundle generation changed during reopen")
        finally:
            shutil.rmtree(staging_root, ignore_errors=True)

        removed_legacy_paths = cleanup_legacy_storage(root)
        result = core.BundleBuildResult(
            path=destination,
            start_session=start_session,
            end_session=end_session,
            asset_count=len(records),
            session_count=len(bundle_sessions),
        )
        if record_status:
            _record_bundle_build(
                root,
                bundle_name=bundle_name,
                csv_dir=source,
                started_at=started_at,
                result=result,
                manifest=manifest,
                removed_legacy_paths=removed_legacy_paths,
            )
        return result
