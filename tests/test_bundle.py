from __future__ import annotations

import json
import shutil
import threading
from pathlib import Path

import h5py
import numpy as np
import pandas as pd
import pytest
from filelock import FileLock

from tualpha import _csv_hdf5
from tualpha._hdf5_store import (
    BUNDLE_PROTOCOL,
    REQUIRED_BUNDLE_FILES,
    load_assets_manifest,
)
from tualpha.bundle import (
    acquire_bundle_read_lock,
    build_bundle,
    bundle_lock_path,
    cleanup_legacy_storage,
    latest_bundle_path,
    load_bundle_data,
    release_bundle_read_lock,
    update_status_path,
)
from tualpha.exceptions import DataError


def test_csv_bucket_worker_materializes_sorted_buckets(
    csv_dir: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(_csv_hdf5, "_WORKER_FILE_THRESHOLD", 0)
    buckets, observations = _csv_hdf5.bucket_daily(
        csv_dir, tmp_path / "buckets", show_progress=False
    )

    assert ("000001.SZ", "stock") in observations
    assert len(list(buckets.root.joinpath("sorted").glob("*.bin"))) == 16
    assert not list(buckets.root.glob("_bucket=*"))


def test_builds_fixed_hdf5_bundle(csv_dir: Path, bundle_root: Path) -> None:
    result = build_bundle(csv_dir, bundle_root=bundle_root, rebuild_normalized=True)

    assert result.asset_count == 4
    assert result.session_count == 5
    assert result.path == bundle_root / "bundle"
    assert latest_bundle_path(bundle_root) == result.path
    assert {path.name for path in result.path.iterdir()} == REQUIRED_BUNDLE_FILES
    assert all(path.is_file() for path in result.path.iterdir())
    assert not any("bcolz" in path.name.lower() for path in result.path.iterdir())

    manifest = load_assets_manifest(result.path / "assets.pk")
    assert manifest["protocol"] == BUNDLE_PROTOCOL
    assert manifest["schema_version"] == 7
    assert manifest["calendar_source"] == "tushare.trade_cal:SSE"
    assert manifest["session_count"] == 5
    assert manifest["asset_count"] == 4
    assert {row["asset_type"] for row in manifest["assets"] if row["tradable"]} == {
        "stock",
        "etf",
    }
    assert np.load(result.path / "trade_dates.npy", allow_pickle=False).tolist() == [
        20240102,
        20240103,
        20240104,
        20240105,
        20240108,
    ]

    with h5py.File(result.path / "daily.h5", "r") as daily:
        assert daily.attrs["protocol"] == BUNDLE_PROTOCOL
        assert daily.attrs["generation"] == manifest["generation"]
        assert set(daily["data"]) >= {"000001.SZ", "510300.SH", "000985.CSI"}
        stock = daily["data"]["000001.SZ"][:]
        assert stock.dtype.names == (
            "trade_date",
            "open",
            "high",
            "low",
            "close",
            "pre_close",
            "volume",
            "turnover",
        )
        assert stock[0]["volume"] == pytest.approx(1_000_076.0)

    with h5py.File(result.path / "daily_basic.h5", "r") as daily_basic:
        values = daily_basic["data"]["000001.SZ"][:]
        assert np.isfinite(values["pe"]).sum() == 5
        fields = json.loads(daily_basic.attrs["fields"])
        assert "pe_ttm" in fields

    with h5py.File(result.path / "stock_st.h5", "r") as stock_st:
        assert stock_st["data"]["000001.SZ"][:]["is_st"].sum() == 1
    with h5py.File(result.path / "finance.h5", "r") as finance:
        assert len(finance["data"]["income"]["000001.SZ"]) == 3
    with h5py.File(result.path / "index_weight.h5", "r") as weights:
        assert sum(len(dataset) for dataset in weights["data"].values()) == 19

    loaded = load_bundle_data(bundle_root)
    try:
        assert loaded.asset_finder.retrieve_asset(1).ts_code == "000001.SZ"
        assert loaded.h5["daily"]["data"]["000001.SZ"][0]["close"] == 10.0
    finally:
        loaded.close()

    status = json.loads(update_status_path(bundle_root).read_text(encoding="utf-8"))
    assert status["operation"] == "bundle_build"
    assert status["active_generation"] == manifest["generation"]
    assert status["last_bundle_build"]["bundle_path"] == str(result.path)


def test_latest_bundle_recovers_interrupted_rollback(data_root: Path) -> None:
    current = latest_bundle_path(data_root)
    previous = data_root / ".rollback" / "interrupted" / "bundle"
    previous.parent.mkdir(parents=True)
    current.replace(previous)
    assert latest_bundle_path(data_root) == current
    assert current.is_dir()
    assert not previous.exists()


def test_legacy_storage_cleanup_is_scoped_to_bundle_root(tmp_path: Path) -> None:
    root = tmp_path / ".tualpha"
    active = root / "bundle"
    active.mkdir(parents=True)
    (active / "keep.txt").write_text("keep", encoding="utf-8")
    legacy_bcolz = root / "bundles" / "tualpha" / "daily_equities.bcolz"
    legacy_bcolz.mkdir(parents=True)
    legacy_cache = root / "cache" / "tualpha" / "normalized.duckdb"
    legacy_cache.parent.mkdir(parents=True)
    legacy_cache.write_bytes(b"legacy")

    removed = cleanup_legacy_storage(root)

    assert len(removed) == 2
    assert active.joinpath("keep.txt").is_file()
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


def test_direct_build_rejects_interrupted_data_update(
    csv_dir: Path, tmp_path: Path
) -> None:
    root = tmp_path / "bundle-root"
    root.mkdir()
    update_status_path(root).write_text(
        json.dumps({"operation": "data_update", "status": "running"}),
        encoding="utf-8",
    )
    with pytest.raises(DataError, match="previous data update was interrupted"):
        build_bundle(csv_dir, bundle_root=root)


def test_bundle_name_and_source_paths_cannot_escape_managed_root(
    csv_dir: Path, tmp_path: Path
) -> None:
    root = tmp_path / "bundle-root"
    escaped = tmp_path / "escaped"
    with pytest.raises(DataError, match="bundle_name"):
        build_bundle(
            csv_dir,
            bundle_root=root,
            bundle_name="../../escaped",
        )
    assert not escaped.exists()

    overlapping_csv = root / "raw-csv"
    with pytest.raises(DataError, match="must not overlap"):
        build_bundle(overlapping_csv, bundle_root=root)


def test_rebuild_reads_csv_directly_without_database_cache(
    csv_dir: Path, tmp_path: Path
) -> None:
    alternate = tmp_path / "alternate-csv"
    shutil.copytree(csv_dir, alternate)
    daily_path = alternate / "daily" / "20240102.csv"
    daily = pd.read_csv(daily_path)
    daily.loc[daily["ts_code"] == "000001.SZ", ["open", "high", "low", "close"]] = 99.0
    daily.to_csv(daily_path, index=False)

    root = tmp_path / "bundle-root"
    build_bundle(csv_dir, bundle_root=root, rebuild_normalized=True)
    build_bundle(alternate, bundle_root=root)

    loaded = load_bundle_data(root)
    try:
        assert loaded.h5["daily"]["data"]["000001.SZ"][0]["close"] == pytest.approx(
            99.0
        )
    finally:
        loaded.close()

    daily = pd.read_csv(daily_path)
    daily.loc[daily["ts_code"] == "000001.SZ", ["open", "high", "low", "close"]] = 7.0
    daily.to_csv(daily_path, index=False)
    build_bundle(alternate, bundle_root=root)
    loaded = load_bundle_data(root)
    try:
        assert loaded.h5["daily"]["data"]["000001.SZ"][0]["close"] == pytest.approx(7.0)
    finally:
        loaded.close()

    assert not (root / "cache").exists()
    assert not list(root.rglob("*.duckdb"))
