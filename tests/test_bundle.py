from __future__ import annotations

import json
import shutil
import sqlite3
import threading
from pathlib import Path

import bcolz
import duckdb
import numpy as np
import pandas as pd
import pytest
from filelock import FileLock

from tualpha.bundle import (
    acquire_bundle_read_lock,
    build_bundle,
    bundle_lock_path,
    bundle_parent,
    latest_bundle_path,
    load_bundle_data,
    release_bundle_read_lock,
    update_status_path,
)
from tualpha.exceptions import DataError


def test_builds_fixed_official_bundle_with_bcolz_extensions(
    csv_dir: Path, bundle_root: Path
) -> None:
    result = build_bundle(csv_dir, bundle_root=bundle_root, rebuild_normalized=True)

    assert result.asset_count == 4
    assert result.session_count == 5
    assert result.path == bundle_root / "bundles" / "tualpha"
    assert latest_bundle_path(bundle_root) == result.path
    assert (result.path / "assets-7.sqlite").is_file()
    assert (result.path / "daily_equities.bcolz").is_dir()
    assert (result.path / "minute_equities.bcolz" / "metadata.json").is_file()
    assert (result.path / "adjustments.sqlite").is_file()
    assert (result.path / "finance.sqlite").is_file()
    assert not (result.path / "tualpha.duckdb").exists()
    assert (result.path / "READY").is_file()

    manifest = json.loads((result.path / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["schema_version"] == 4
    assert manifest["settlement_days"] == 1
    assert manifest["volume_multiplier"] == 100.0
    assert manifest["finance"] == "finance.sqlite"
    assert "000985.CSI" in manifest["benchmark_sids"]

    daily = bcolz.open(rootdir=str(result.path / "daily_equities.bcolz"), mode="r")
    assert "ta_daily_basic__pe" in daily.names
    assert "ta_moneyflow__net_mf_amount" in daily.names
    assert "ta_industry__l1_name" in daily.names
    assert "ta_stock_st__is_st" in daily.names
    assert all(len(daily[name]) == len(daily) for name in daily.names)
    assert len(np.unique(daily["id"][:])) == 4
    assert np.isfinite(daily["ta_daily_basic__pe"][:]).sum() == 5
    assert daily["ta_stock_st__is_st"][:].sum() == 1
    field_registry = daily.attrs["tualpha_fields"]
    assert "daily_basic.pe_ttm" in field_registry
    industry_spec = field_registry["industry.l1_name"]
    assert industry_spec["kind"] == "categorical"
    assert "银行" in industry_spec["categories"]
    assert daily[industry_spec["column"]].dtype == np.dtype("int32")
    index_daily = bcolz.open(rootdir=str(result.path / "index_daily.bcolz"), mode="r")
    assert len(index_daily) == 5
    assert len(np.unique(index_daily["id"][:])) == 1

    asset_db = sqlite3.connect(result.path / "assets-7.sqlite")
    try:
        assert (
            asset_db.execute(
                "SELECT count(*) FROM equity_supplementary_mappings "
                "WHERE field = 'asset_type' AND value = 'etf'"
            ).fetchone()[0]
            == 2
        )
    finally:
        asset_db.close()

    finance = sqlite3.connect(result.path / "finance.sqlite")
    try:
        assert finance.execute("PRAGMA integrity_check").fetchone() == ("ok",)
        assert (
            finance.execute("SELECT count(*) FROM financial_income").fetchone()[0] == 3
        )
    finally:
        finance.close()

    loaded = load_bundle_data(bundle_root)
    try:
        asset = loaded.asset_finder.retrieve_asset(1)
        assert asset is not None
        assert loaded.equity_daily_bar_reader.get_value(
            1, pd.Timestamp("2024-01-02"), "volume"
        ) == pytest.approx(1_000_076.0)
    finally:
        loaded.close()

    published = [
        path
        for path in bundle_parent(bundle_root).iterdir()
        if path.is_dir() and not path.name.startswith(".")
    ]
    assert published == [result.path]
    assert all(not path.name[:4].isdigit() for path in published)
    status = json.loads(update_status_path(bundle_root).read_text(encoding="utf-8"))
    assert status["operation"] == "bundle_build"
    assert status["last_bundle_build"]["bundle_path"] == str(result.path)


def test_latest_bundle_falls_back_to_interrupted_previous(
    data_root: Path,
) -> None:
    current = latest_bundle_path(data_root)
    previous = current.parent / ".previous-tualpha-interrupted"
    current.replace(previous)
    try:
        assert latest_bundle_path(data_root) == previous
    finally:
        previous.replace(current)


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


def test_normalized_cache_is_not_reused_for_another_csv_source(
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
        assert loaded.equity_daily_bar_reader.get_value(
            1, pd.Timestamp("2024-01-02"), "close"
        ) == pytest.approx(99.0)
    finally:
        loaded.close()

    daily = pd.read_csv(daily_path)
    daily.loc[daily["ts_code"] == "000001.SZ", ["open", "high", "low", "close"]] = 7.0
    daily.to_csv(daily_path, index=False)
    build_bundle(alternate, bundle_root=root)
    loaded = load_bundle_data(root)
    try:
        assert loaded.equity_daily_bar_reader.get_value(
            1, pd.Timestamp("2024-01-02"), "close"
        ) == pytest.approx(7.0)
    finally:
        loaded.close()

    connection = duckdb.connect(
        str(root / "cache" / "tualpha" / "normalized.duckdb"),
        read_only=True,
    )
    try:
        cached_source = connection.execute(
            "SELECT value FROM store_metadata WHERE key = 'csv_dir'"
        ).fetchone()[0]
    finally:
        connection.close()
    assert Path(cached_source) == alternate.resolve()
