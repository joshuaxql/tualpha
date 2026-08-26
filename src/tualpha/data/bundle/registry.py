"""Bundle coordination, stable assets, and publication locks."""

from __future__ import annotations

import json
import os
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from threading import Lock

import pandas as pd
from filelock import FileLock

from ._hdf5_store import load_assets_manifest
from .config import DEFAULT_BUNDLE_ROOT
from .exceptions import DataError

BUNDLE_NAME = "tualpha"
SID_MAP_VERSION = 1
_BUNDLE_NAME_PATTERN = re.compile(r"(?=.{1,64}\Z)[A-Za-z0-9]+(?:[._-][A-Za-z0-9]+)*\Z")
_READ_LOCK_GUARD = Lock()
_READ_LOCKS: dict[str, tuple[FileLock, int]] = {}


@dataclass(frozen=True, slots=True)
class BundleAssetRecord:
    sid: int
    ts_code: str
    name: str
    asset_type: str
    exchange: str
    board: str
    price_tick: float
    start_date: pd.Timestamp
    end_date: pd.Timestamp


@dataclass(frozen=True, slots=True)
class BundleBuildResult:
    path: Path
    start_session: pd.Timestamp
    end_session: pd.Timestamp
    asset_count: int
    session_count: int


def validate_bundle_name(bundle_name: str) -> str:
    if not _BUNDLE_NAME_PATTERN.fullmatch(bundle_name):
        raise DataError(
            "bundle_name must contain only letters, digits, '.', '_' or '-', "
            "must not contain path separators, and must be at most 64 characters"
        )
    return bundle_name


_legacy_validate_bundle_name = validate_bundle_name


def paths_overlap(first: str | Path, second: str | Path) -> bool:
    left = os.path.normcase(str(Path(first).expanduser().resolve()))
    right = os.path.normcase(str(Path(second).expanduser().resolve()))
    try:
        common = os.path.normcase(os.path.commonpath([left, right]))
    except ValueError:
        return False
    return common == left or common == right


def bundle_lock_path(
    bundle_root: str | Path = DEFAULT_BUNDLE_ROOT,
    bundle_name: str = BUNDLE_NAME,
) -> Path:
    validate_bundle_name(bundle_name)
    return Path(bundle_root).expanduser() / ".locks" / "bundle.lock"


def acquire_bundle_read_lock(
    bundle_root: str | Path = DEFAULT_BUNDLE_ROOT,
    bundle_name: str = BUNDLE_NAME,
) -> tuple[str, FileLock]:
    path = bundle_lock_path(bundle_root, bundle_name)
    path.parent.mkdir(parents=True, exist_ok=True)
    key = os.path.normcase(str(path.resolve()))
    with _READ_LOCK_GUARD:
        existing = _READ_LOCKS.get(key)
        if existing is not None:
            lock, count = existing
            _READ_LOCKS[key] = (lock, count + 1)
            return key, lock
        lock = FileLock(str(path), thread_local=False)
        lock.acquire()
        _READ_LOCKS[key] = (lock, 1)
        return key, lock


def release_bundle_read_lock(key: str) -> None:
    with _READ_LOCK_GUARD:
        lock, count = _READ_LOCKS[key]
        if count > 1:
            _READ_LOCKS[key] = (lock, count - 1)
            return
        lock.release()
        del _READ_LOCKS[key]


def update_status_path(bundle_root: str | Path = DEFAULT_BUNDLE_ROOT) -> Path:
    return Path(bundle_root).expanduser() / "update-status.json"


class SidRegistry:
    """Stable sid mapping recovered from the active Bundle or legacy sid map."""

    def __init__(self, bundle_root: Path) -> None:
        self.bundle_root = bundle_root
        self.mapping: dict[str, int] = {}
        active = bundle_root / "bundle" / "assets.pk"
        if active.is_file():
            manifest = load_assets_manifest(active)
            self.mapping = {
                str(asset["ts_code"]).upper(): int(asset["sid"])
                for asset in manifest["assets"]
            }
            return
        legacy = bundle_root / "cache" / "tualpha" / "sid-map.json"
        if legacy.is_file():
            payload = json.loads(legacy.read_text(encoding="utf-8"))
            if payload.get("version") != SID_MAP_VERSION:
                raise DataError(f"unsupported sid map version in {legacy}")
            self.mapping = {
                str(code).upper(): int(sid)
                for code, sid in payload.get("assets", {}).items()
            }

    def assign(self, codes: Sequence[str]) -> dict[str, int]:
        next_sid = max(self.mapping.values(), default=0) + 1
        for code in sorted({str(value).upper() for value in codes}):
            if code not in self.mapping:
                self.mapping[code] = next_sid
                next_sid += 1
        return {str(code).upper(): self.mapping[str(code).upper()] for code in codes}


def _parse_date(value: object) -> pd.Timestamp | None:
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    try:
        return pd.to_datetime(text, format="%Y%m%d").normalize()
    except ValueError as exc:
        raise DataError(f"invalid asset date: {value}") from exc


def load_bundle_assets(
    csv_dir: Path,
    observations: Mapping[tuple[str, str], tuple[pd.Timestamp, pd.Timestamp]],
    sid_registry: SidRegistry,
    bundle_end: pd.Timestamp,
) -> list[BundleAssetRecord]:
    stock_frame = pd.read_csv(
        csv_dir / "stock_basic.csv", dtype=str, keep_default_na=False
    )
    etf_frame = pd.read_csv(csv_dir / "etf_basic.csv", dtype=str, keep_default_na=False)
    drafts: list[dict[str, object]] = []
    board_map = {
        "主板": "main",
        "创业板": "chinext",
        "科创板": "star",
        "北交所": "bse",
    }
    exchange_map = {
        "SSE": "SSE",
        "SZSE": "SZSE",
        "BSE": "BSE",
        "SH": "SSE",
        "SZ": "SZSE",
    }
    for row in stock_frame.to_dict("records"):
        code = str(row.get("ts_code", "")).upper()
        observed = observations.get((code, "stock"))
        if not code or observed is None:
            continue
        first_observed, _ = observed
        list_date = _parse_date(row.get("list_date")) or first_observed
        delist_date = _parse_date(row.get("delist_date"))
        end_date = (
            min(bundle_end, delist_date) if delist_date is not None else bundle_end
        )
        drafts.append(
            {
                "ts_code": code,
                "name": str(row.get("name", "")),
                "asset_type": "stock",
                "exchange": exchange_map.get(str(row.get("exchange", "")), "SSE"),
                "board": board_map.get(str(row.get("market", "")), "unknown"),
                "price_tick": 0.01,
                "start_date": max(list_date, first_observed),
                "end_date": end_date,
            }
        )
    for row in etf_frame.to_dict("records"):
        code = str(row.get("ts_code", "")).upper()
        observed = observations.get((code, "etf"))
        if not code or observed is None:
            continue
        first_observed, last_observed = observed
        list_date = _parse_date(row.get("list_date")) or first_observed
        end_date = (
            bundle_end if str(row.get("list_status", "")) == "L" else last_observed
        )
        drafts.append(
            {
                "ts_code": code,
                "name": str(row.get("extname") or row.get("csname") or ""),
                "asset_type": "etf",
                "exchange": exchange_map.get(str(row.get("exchange", "")), "SSE"),
                "board": "etf",
                "price_tick": 0.001,
                "start_date": max(list_date, first_observed),
                "end_date": min(bundle_end, end_date),
            }
        )
    sid_map = sid_registry.assign([str(draft["ts_code"]) for draft in drafts])
    return [
        BundleAssetRecord(sid=sid_map[str(draft["ts_code"])], **draft)
        for draft in drafts
        if draft["start_date"] <= draft["end_date"]
    ]


def load_benchmark_assets(
    observations: Mapping[tuple[str, str], tuple[pd.Timestamp, pd.Timestamp]],
    sid_registry: SidRegistry,
    bundle_start: pd.Timestamp,
    bundle_end: pd.Timestamp,
) -> list[BundleAssetRecord]:
    codes = sorted(code for (code, asset_type) in observations if asset_type == "index")
    sid_map = sid_registry.assign(codes)
    records: list[BundleAssetRecord] = []
    for code in codes:
        first_date, last_date = observations[(code, "index")]
        start_date = max(bundle_start, first_date)
        end_date = min(bundle_end, last_date)
        if start_date > end_date:
            continue
        suffix = code.rsplit(".", 1)[-1] if "." in code else "INDEX"
        records.append(
            BundleAssetRecord(
                sid=sid_map[code],
                ts_code=code,
                name=code,
                asset_type="index",
                exchange=suffix,
                board="index",
                price_tick=0.01,
                start_date=start_date,
                end_date=end_date,
            )
        )
    return records


def reject_interrupted_data_update(root: Path) -> None:
    path = update_status_path(root)
    if not path.is_file():
        return
    try:
        status = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return
    if status.get("operation") == "data_update" and status.get("status") == "running":
        raise DataError(
            "a previous data update was interrupted; run `tualpha update` to "
            "recover CSV files before building a Bundle"
        )
