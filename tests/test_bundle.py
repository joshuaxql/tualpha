from __future__ import annotations

import shutil
import threading
from pathlib import Path

import pytest
from filelock import FileLock

from tualpha.bundle import (
    acquire_bundle_read_lock,
    bundle_lock_path,
    cleanup_legacy_storage,
    latest_bundle_path,
    load_bundle_data,
    release_bundle_read_lock,
    validate_bundle_name,
    validate_parquet_bundle,
)
from tualpha.data.bundle.parquet_store import load_manifest
from tualpha.exceptions import DataError


def test_bundle_contains_catalog_manifest_and_parquet(bundle_root: Path) -> None:
    path = latest_bundle_path(bundle_root)
    manifest = validate_parquet_bundle(path, full_hash=True)

    assert {item.name for item in path.iterdir()} == {
        "catalog.duckdb",
        "manifest.json",
        "parquet",
    }
    assert manifest["protocol"] == "tualpha.parquet/1"
    assert manifest["schema_version"] == 1
    assert manifest["calendar_source"] == "tushare.trade_cal:SSE"
    assert manifest["session_count"] == 5
    assert manifest["asset_count"] == 4
    assert not list(path.rglob("*.h5"))

    with load_bundle_data(bundle_root) as loaded:
        assert loaded.asset_finder.retrieve_asset(1).ts_code == "000001.SZ"
        daily = loaded.client.query(
            "stock_daily",
            filters={"ts_code": "000001.SZ", "trade_date": "20240102"},
        )
        assert daily.iloc[0]["close"] == 10.0


def test_latest_bundle_recovers_interrupted_parquet_rollback(data_root: Path) -> None:
    current = latest_bundle_path(data_root)
    previous = data_root / ".rollback" / "interrupted" / "bundle"
    previous.parent.mkdir(parents=True)
    current.replace(previous)

    assert latest_bundle_path(data_root) == current
    assert current.is_dir()
    assert not previous.exists()


def test_legacy_storage_cleanup_keeps_active_duckdb(tmp_path: Path) -> None:
    root = tmp_path / ".tualpha"
    active = root / "bundle"
    active.mkdir(parents=True)
    catalog = active / "catalog.duckdb"
    catalog.write_bytes(b"active")
    legacy_bcolz = root / "bundles" / "tualpha" / "daily_equities.bcolz"
    legacy_bcolz.mkdir(parents=True)
    legacy_cache = root / "cache" / "old.sqlite"
    legacy_cache.parent.mkdir(parents=True)
    legacy_cache.write_bytes(b"legacy")

    removed = cleanup_legacy_storage(root)

    assert len(removed) == 2
    assert catalog.is_file()
    assert not root.joinpath("bundles").exists()
    assert not root.joinpath("cache").exists()


def test_bundle_read_lock_releases_across_threads(tmp_path: Path) -> None:
    first_acquired = threading.Event()
    second_acquired = threading.Event()
    first_released = threading.Event()

    def first_reader() -> None:
        key, _ = acquire_bundle_read_lock(tmp_path)
        first_acquired.set()
        second_acquired.wait(timeout=5)
        release_bundle_read_lock(key)
        first_released.set()

    def second_reader() -> None:
        first_acquired.wait(timeout=5)
        key, _ = acquire_bundle_read_lock(tmp_path)
        second_acquired.set()
        first_released.wait(timeout=5)
        release_bundle_read_lock(key)

    threads = [
        threading.Thread(target=first_reader),
        threading.Thread(target=second_reader),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)
        assert not thread.is_alive()

    competing = FileLock(str(bundle_lock_path(tmp_path)), thread_local=False)
    competing.acquire(timeout=0)
    competing.release()


def test_manifest_and_catalog_generation_must_match(
    data_root: Path, tmp_path: Path
) -> None:
    root = tmp_path / "mixed"
    shutil.copytree(latest_bundle_path(data_root), root / "bundle")
    manifest = load_manifest(root / "bundle")
    manifest["generation"] = "different"
    import json

    (root / "bundle/manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(DataError, match="incomplete"):
        latest_bundle_path(root)


def test_bundle_name_cannot_escape_managed_root() -> None:
    with pytest.raises(DataError, match="bundle_name"):
        validate_bundle_name("../../escaped")
