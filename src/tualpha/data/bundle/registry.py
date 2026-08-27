"""Bundle locks, stable asset identifiers, and publication coordination."""

from __future__ import annotations

import json
import os
import re
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from threading import Lock

import duckdb
import pandas as pd
from filelock import FileLock

from ...foundation.config import DEFAULT_BUNDLE_ROOT
from ...foundation.exceptions import DataError
from .legacy_manifest import load_legacy_assets_manifest

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
    """Recover stable sid mappings from the active Bundle or legacy sid map."""

    def __init__(self, bundle_root: Path) -> None:
        self.bundle_root = bundle_root
        self.mapping: dict[str, int] = {}
        catalog = bundle_root / "bundle" / "catalog.duckdb"
        if catalog.is_file():
            connection = duckdb.connect(str(catalog), read_only=True)
            try:
                self.mapping = {
                    str(code).upper(): int(sid)
                    for code, sid in connection.execute(
                        "SELECT ts_code, sid FROM assets"
                    ).fetchall()
                }
            finally:
                connection.close()
            return
        active = bundle_root / "bundle" / "assets.pk"
        if active.is_file():
            manifest = load_legacy_assets_manifest(active)
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
