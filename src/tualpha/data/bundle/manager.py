"""Immutable Parquet Bundle validation, locking, recovery, and publication."""

from __future__ import annotations

import gc
import os
import shutil
import time
from pathlib import Path
from typing import Any, Self
from uuid import uuid4

import pandas as pd

from ...config import DEFAULT_BUNDLE_ROOT
from ...exceptions import DataError
from . import parquet_store, registry
from .parquet_store import load_manifest, validate_bundle

BUNDLE_NAME = "tualpha"
BUNDLE_SCHEMA_VERSION = parquet_store.BUNDLE_SCHEMA_VERSION
UPDATE_STATUS_SCHEMA_VERSION = 7


def validate_bundle_name(bundle_name: str) -> str:
    return registry.validate_bundle_name(bundle_name)


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
    return registry.bundle_lock_path(bundle_root, bundle_name)


def update_status_path(bundle_root: str | Path = DEFAULT_BUNDLE_ROOT) -> Path:
    return registry.update_status_path(bundle_root)


def latest_bundle_path(
    bundle_root: str | Path = DEFAULT_BUNDLE_ROOT,
    bundle_name: str = BUNDLE_NAME,
) -> Path:
    root = Path(bundle_root).expanduser()
    destination = bundle_path(root, bundle_name)
    recover_interrupted_bundle(root)
    try:
        validate_bundle(destination, full_hash=False)
    except DataError as exc:
        raise DataError(
            "Parquet bundle does not exist or is incomplete; run `tualpha build` "
            f"or `tualpha update`: {destination}"
        ) from exc
    return destination


def validate_parquet_bundle(
    path: str | Path,
    *,
    full_hash: bool = True,
    full_scan: bool = True,
) -> dict[str, Any]:
    del full_scan
    return validate_bundle(path, full_hash=full_hash)


# Compatibility name retained for callers upgrading from the HDF5 release.
validate_hdf5_bundle = validate_parquet_bundle


class LockedBundleData:
    """Read-locked Parquet Bundle components and local DuckDB client."""

    def __init__(
        self,
        bundle_root: str | Path,
        bundle_name: str = BUNDLE_NAME,
    ) -> None:
        self._lock_key, _ = registry.acquire_bundle_read_lock(bundle_root, bundle_name)
        try:
            self.path = latest_bundle_path(bundle_root, bundle_name)
            self.manifest = load_manifest(self.path)
            from ...model.asset import AssetFinder
            from ..query import LocalDataClient
            from ..trading_calendar import ChinaTradingCalendar

            self.asset_finder = AssetFinder(bundle_root, bundle_name)
            self.calendar = ChinaTradingCalendar(bundle_root, bundle_name)
            self.client = LocalDataClient(bundle_root, bundle_name)
        except Exception:
            registry.release_bundle_read_lock(self._lock_key)
            self._lock_key = None
            raise

    def close(self) -> None:
        client = getattr(self, "client", None)
        if client is not None:
            client.close()
            self.client = None
        if getattr(self, "_lock_key", None) is not None:
            registry.release_bundle_read_lock(self._lock_key)
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


def recover_interrupted_bundle(root: Path) -> None:
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
            validate_bundle(candidate, full_hash=False)
        except DataError:
            continue
        _replace_directory(candidate, destination)
        break


def _legacy_generation(path: Path) -> str:
    assets = path / "assets.pk"
    if assets.is_file():
        try:
            from .legacy_manifest import load_legacy_assets_manifest

            return str(load_legacy_assets_manifest(assets).get("generation", "unknown"))
        except Exception:  # noqa: BLE001 - legacy backup naming is best effort
            return f"unknown-{pd.Timestamp.utcnow().strftime('%Y%m%d%H%M%S')}"
    return f"unknown-{pd.Timestamp.utcnow().strftime('%Y%m%d%H%M%S')}"


def publish_fixed_bundle(staged: Path, destination: Path) -> None:
    """Atomically publish a validated generation and retain the first HDF5 Bundle."""

    validate_bundle(staged, full_hash=False)
    root = destination.parent
    rollback = root / ".rollback" / uuid4().hex / "bundle"
    rollback.parent.mkdir(parents=True, exist_ok=False)
    moved_previous = False
    legacy_backup: Path | None = None
    try:
        if destination.exists():
            if (
                not (destination / "manifest.json").is_file()
                and (destination / "assets.pk").is_file()
            ):
                legacy_backup = (
                    root / "backups" / f"hdf5-{_legacy_generation(destination)}"
                )
                legacy_backup.parent.mkdir(parents=True, exist_ok=True)
                if legacy_backup.exists():
                    raise DataError(
                        f"legacy HDF5 backup already exists: {legacy_backup}"
                    )
                _replace_directory(destination, legacy_backup)
            else:
                _replace_directory(destination, rollback)
                moved_previous = True
        _replace_directory(staged, destination)
        validate_bundle(destination, full_hash=False)
    except Exception:
        if destination.exists():
            shutil.rmtree(destination, ignore_errors=True)
        if moved_previous and rollback.exists():
            _replace_directory(rollback, destination)
        elif legacy_backup is not None and legacy_backup.exists():
            _replace_directory(legacy_backup, destination)
        raise
    else:
        shutil.rmtree(rollback.parent, ignore_errors=True)


def cleanup_legacy_storage(bundle_root: str | Path) -> tuple[str, ...]:
    """Remove only obsolete Bcolz/SQLite caches; Parquet and DuckDB are active data."""

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
    for pattern in ("*.bcolz", "*.sqlite"):
        for target in root.rglob(pattern):
            if target.is_symlink() or root not in target.resolve().parents:
                raise DataError(f"refusing to remove unsafe legacy path: {target}")
            if target.is_dir():
                shutil.rmtree(target)
            else:
                target.unlink()
            removed.append(str(target))
    return tuple(removed)


acquire_bundle_read_lock = registry.acquire_bundle_read_lock
release_bundle_read_lock = registry.release_bundle_read_lock
paths_overlap = registry.paths_overlap
