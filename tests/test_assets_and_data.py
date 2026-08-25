from __future__ import annotations

import shutil
from importlib.util import find_spec
from pathlib import Path

import h5py
import pandas as pd
import pytest

from tualpha import _data_v8
from tualpha._hdf5_store import (
    HDF5_FILES,
    REQUIRED_BUNDLE_FILES,
    load_assets_manifest,
    write_assets_manifest,
)
from tualpha.assets import AssetFinder, Board
from tualpha.bundle import latest_bundle_path
from tualpha.calendar import ChinaTradingCalendar
from tualpha.data import BarData, TushareDataPortal
from tualpha.exceptions import DataError


def test_asset_metadata_and_all_assets_are_t1(data_root: Path) -> None:
    finder = AssetFinder(data_root)

    assert finder.retrieve_asset("688001.SH").board is Board.STAR
    assert finder.retrieve_asset("510300.SH").settlement_days == 1
    assert finder.retrieve_asset("513100.SH").settlement_days == 1
    assert all(asset.settlement_days == 1 for asset in finder)
    assert finder.retrieve_asset("510300").ts_code == "510300.SH"
    with pytest.raises(LookupError):
        finder.retrieve_asset("160001.SZ")


def test_extended_daily_fields_are_available_without_csv_reads(data_root: Path) -> None:
    finder = AssetFinder(data_root)
    calendar = ChinaTradingCalendar(data_root)
    stock = finder.retrieve_asset("000001.SZ")
    portal = TushareDataPortal(data_root, finder, calendar, "raw", "2024-01-08")

    assert portal.value(stock, "2024-01-02", "daily_basic.pe") == 10.0
    assert portal.value(stock, "2024-01-02", "moneyflow.net_mf_amount") == 1.0
    assert portal.value(stock, "2024-01-02", "industry.l1_name") == "银行"
    assert portal.value(stock, "2024-01-03", "stock_st.is_st") == 0.0
    assert portal.value(stock, "2024-01-04", "stock_st.is_st") == 1.0
    assert portal.value(stock, "2024-01-04", "stock_st.name") == "ST测试"

    def fail_session_loop(*args: object, **kwargs: object) -> object:
        raise AssertionError("history must gather the complete matrix without values()")

    portal.values = fail_session_loop  # type: ignore[method-assign]
    history = portal.history(
        [stock],
        ["daily_basic.pe", "industry.l1_name", "stock_st.is_st"],
        "2024-01-04",
        3,
    )
    assert history[(stock.ts_code, "daily_basic.pe")].tolist() == [10.0] * 3
    assert history[(stock.ts_code, "daily_basic.pe")].dtype == "float64"
    assert history[(stock.ts_code, "industry.l1_name")].tolist() == ["银行"] * 3
    assert history[(stock.ts_code, "stock_st.is_st")].tolist() == [0.0, 0.0, 1.0]
    assert "daily_basic.pe_ttm" in portal.available_fields("daily_basic")
    with pytest.raises(KeyError, match="unknown daily field"):
        portal.value(stock, "2024-01-02", "daily_basic.missing")
    portal.close()


@pytest.mark.parametrize("adjustment", ["raw", "qfq", "hfq"])
def test_vectorized_current_matches_scalar_values_and_uses_bounded_cache(
    data_root: Path,
    adjustment: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    finder = AssetFinder(data_root)
    calendar = ChinaTradingCalendar(data_root)
    assets = [
        finder.retrieve_asset(code)
        for code in ("000001.SZ", "688001.SH", "510300.SH", "513100.SH")
    ]
    fields = [
        "open",
        "close",
        "pre_close",
        "volume",
        "up_limit",
        "down_limit",
        "suspended",
        "daily_basic.pe",
        "industry.l1_name",
        "stock_st.is_st",
    ]
    portal = TushareDataPortal(data_root, finder, calendar, adjustment, "2024-01-08")
    data = BarData(portal)
    data._set_session("2024-01-04")
    frame = data.current(assets, fields)

    for asset in assets:
        for field in fields:
            actual = frame.loc[asset.ts_code, field]
            expected = portal.value(asset, "2024-01-04", field)
            if expected is None:
                assert actual is None
            elif isinstance(expected, str):
                assert actual == expected
            elif pd.isna(expected):
                assert pd.isna(actual)
            else:
                assert float(actual) == pytest.approx(float(expected))

    original = frame.loc["000001.SZ", "daily_basic.pe"]
    frame.loc["000001.SZ", "daily_basic.pe"] = -999.0

    def fail_scalar(*args: object, **kwargs: object) -> object:
        raise AssertionError("multi-asset current must not call scalar portal.value")

    monkeypatch.setattr(portal, "value", fail_scalar)
    fresh = data.current(assets, fields)
    assert fresh.loc["000001.SZ", "daily_basic.pe"] == original
    arrays = data.current_arrays(assets, fields)
    assert arrays["close"].tolist() == pytest.approx(
        frame["close"].astype(float).tolist(), nan_ok=True
    )
    assert all(not values.flags.writeable for values in arrays.values())
    with pytest.raises(ValueError, match="read-only"):
        arrays["close"][0] = -1.0

    assert portal._column_cache
    assert portal._column_cache_bytes <= portal._column_cache_max_bytes
    assert all(not values.flags.writeable for values in portal._column_cache.values())
    assert portal._query_plan(assets) is portal._query_plan(tuple(assets))
    portal.clear_cache()
    assert not portal._column_cache
    assert portal._column_cache_bytes == 0
    portal.close()


def test_schema7_reader_falls_back_when_packed_acceleration_is_absent(
    data_root: Path, tmp_path: Path
) -> None:
    fallback_root = tmp_path / "fallback"
    fallback_bundle = fallback_root / "bundle"
    shutil.copytree(latest_bundle_path(data_root), fallback_bundle)
    manifest = load_assets_manifest(fallback_bundle / "assets.pk")
    manifest.pop("packed_acceleration", None)
    for filename in HDF5_FILES:
        with h5py.File(fallback_bundle / filename, "r+") as handle:
            if "packed" in handle:
                del handle["packed"]
            for attribute in ("packed_layout", "packed_fields"):
                if attribute in handle.attrs:
                    del handle.attrs[attribute]
        manifest["files"][filename]["size"] = (
            (fallback_bundle / filename).stat().st_size
        )
    write_assets_manifest(fallback_bundle / "assets.pk", manifest)

    finder = AssetFinder(fallback_root)
    calendar = ChinaTradingCalendar(fallback_root)
    stock = finder.retrieve_asset("000001.SZ")
    portal = TushareDataPortal(fallback_root, finder, calendar, "raw", "2024-01-08")
    assert portal.value(stock, "2024-01-03", "close") == 11.0
    assert pd.isna(portal.value(stock, "2024-01-03", "daily_basic.total_mv"))
    portal.close()


def test_packed_hot_columns_do_not_scan_per_security_datasets(
    data_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    finder = AssetFinder(data_root)
    calendar = ChinaTradingCalendar(data_root)
    assets = [
        finder.retrieve_asset(code) for code in ("000001.SZ", "688001.SH", "510300.SH")
    ]
    portal = TushareDataPortal(data_root, finder, calendar, "raw", "2024-01-08")

    def fail_per_security_read(*args: object, **kwargs: object) -> object:
        raise AssertionError("packed hot columns must not scan data/<ts_code>")

    monkeypatch.setattr(_data_v8, "_field_array", fail_per_security_read)
    values = portal.values(
        assets,
        "2024-01-03",
        [
            "open",
            "close",
            "volume",
            "up_limit",
            "down_limit",
            "suspended",
            "daily_basic.total_mv",
            "stock_st.is_st",
        ],
    )
    assert values["close"].tolist() == pytest.approx([11.0, 20.0, 4.0])
    portal.close()


def test_batch_execution_bars_match_scalar_market_fields(data_root: Path) -> None:
    finder = AssetFinder(data_root)
    calendar = ChinaTradingCalendar(data_root)
    assets = [
        finder.retrieve_asset(code) for code in ("000001.SZ", "688001.SH", "510300.SH")
    ]
    portal = TushareDataPortal(data_root, finder, calendar, "raw", "2024-01-08")
    bars = portal.execution_bars(assets, "2024-01-03", "open")
    for asset in assets:
        scalar = portal.raw_bar(asset, "2024-01-03")
        batch = bars[asset]
        assert (batch is None) is (scalar is None)
        if scalar is None:
            continue
        assert batch is not None
        assert batch.open == pytest.approx(scalar.open, nan_ok=True)
        assert batch.volume == pytest.approx(scalar.volume)
        assert batch.up_limit == pytest.approx(scalar.up_limit, nan_ok=True)
        assert batch.down_limit == pytest.approx(scalar.down_limit, nan_ok=True)
        assert batch.suspended is scalar.suspended
    portal.close()


def test_financial_queries_are_announcement_point_in_time(data_root: Path) -> None:
    finder = AssetFinder(data_root)
    calendar = ChinaTradingCalendar(data_root)
    stock = finder.retrieve_asset("000001.SZ")
    portal = TushareDataPortal(data_root, finder, calendar, "raw", "2024-01-08")

    assert pd.isna(portal.fundamental(stock, "2024-01-02", "income.revenue"))
    assert portal.fundamental(stock, "2024-01-03", "income.revenue") == 100.0
    assert portal.fundamental(stock, "2024-01-04", "income.revenue") == 100.0
    assert portal.fundamental(stock, "2024-01-05", "income.revenue") == 110.0
    assert portal.fundamental(stock, "2024-01-05", "fina_indicator.roe") == 11.0
    assert portal.fundamental(stock, "2024-01-08", "income.revenue") == 120.0
    assert "fina_indicator.invturn_days" in portal.available_fields("fina_indicator")
    assert pd.isna(
        portal.fundamental(stock, "2024-01-08", "fina_indicator.invturn_days")
    )
    assert (
        portal.fundamental(
            stock,
            "2024-01-08",
            "income.revenue",
            period="20230930",
        )
        == 110.0
    )

    frame = portal.fundamentals(
        stock,
        "2024-01-08",
        [
            "fina_indicator.roe",
            "income.revenue",
            "balancesheet.total_assets",
            "cashflow.n_cashflow_act",
        ],
        periods=2,
    )
    assert list(frame.index.strftime("%Y%m%d")) == ["20231231", "20230930"]
    assert frame.loc[pd.Timestamp("2023-12-31"), "fina_indicator.roe"] == 12.0
    assert frame.loc[pd.Timestamp("2023-09-30"), "income.revenue"] == 110.0
    assert frame.loc[pd.Timestamp("2023-09-30"), "balancesheet.total_assets"] == 1100.0
    assert frame.loc[pd.Timestamp("2023-09-30"), "cashflow.n_cashflow_act"] == 11.0

    data = BarData(portal)
    data._set_session("2024-01-04")
    assert data.fundamental(stock, "income.revenue") == 100.0
    assert data.fundamentals(stock, "fina_indicator.roe", periods=2).iloc[0, 0] == 10.0
    with pytest.raises(KeyError, match="must be read with fundamental"):
        data.current(stock, "income.revenue")
    portal.close()


def test_index_constituents_are_strictly_point_in_time(data_root: Path) -> None:
    finder = AssetFinder(data_root)
    calendar = ChinaTradingCalendar(data_root)
    portal = TushareDataPortal(data_root, finder, calendar, "raw", "2024-01-08")

    before_visible = portal.index_constituents("000300.SH", "2024-01-02")
    assert before_visible.empty
    assert list(before_visible.columns) == ["asset", "weight", "snapshot_date"]

    initial = portal.index_constituents("000300.SH", "2024-01-03")
    assert list(initial.index) == ["000001.SZ", "688001.SH"]
    assert initial["weight"].tolist() == [60.0, 40.0]
    assert initial.loc["000001.SZ", "asset"] == finder.retrieve_asset("000001.SZ")
    assert initial.attrs["snapshot_date"] == pd.Timestamp("2024-01-02")

    same_day = portal.index_constituents("000300.SH", "2024-01-04")
    assert list(same_day.index) == ["000001.SZ", "688001.SH"]
    next_day = portal.index_constituents("000300.SH", "2024-01-05")
    assert list(next_day.index) == ["688001.SH"]
    assert next_day.iloc[0]["weight"] == 100.0
    assert next_day.attrs["snapshot_date"] == pd.Timestamp("2024-01-04")

    initial.loc["000001.SZ", "weight"] = 0.0
    assert (
        portal.index_constituents("000300.SH", "2024-01-03").loc["000001.SZ", "weight"]
        == 60.0
    )

    data = BarData(portal)
    data._set_session("2024-01-05")
    assert list(data.index_constituents("000300.SH").index) == ["688001.SH"]
    with pytest.raises(KeyError, match="unavailable"):
        data.index_constituents("MISSING.INDEX")
    with pytest.raises(DataError, match="cannot exceed backtest end"):
        portal.index_constituents("000300.SH", "2024-01-09")
    portal.close()


def test_bundle_has_only_schema7_protocol_files(data_root: Path) -> None:
    path = latest_bundle_path(data_root)
    assert {entry.name for entry in path.iterdir()} == REQUIRED_BUNDLE_FILES
    assert not any("bcolz" in entry.name.lower() for entry in path.iterdir())


def test_portal_rejects_mixed_hdf5_generation(data_root: Path, tmp_path: Path) -> None:
    mixed_root = tmp_path / "mixed-generation"
    mixed_path = mixed_root / "bundle"
    shutil.copytree(latest_bundle_path(data_root), mixed_path)
    with h5py.File(mixed_path / "daily.h5", "r+") as handle:
        handle.attrs["generation"] = "stale-generation"
    with pytest.raises(DataError, match="incomplete"):
        AssetFinder(mixed_root)


def test_portal_rejects_mixed_bundle_generations(data_root: Path) -> None:
    finder = AssetFinder(data_root)
    calendar = ChinaTradingCalendar(data_root)
    finder.bundle_generation = "stale-generation"
    with pytest.raises(DataError, match="different generations"):
        TushareDataPortal(data_root, finder, calendar, "raw", "2024-01-08")


def test_runtime_has_no_duckdb_dependency(data_root: Path) -> None:
    assert find_spec("duckdb") is None
    finder = AssetFinder(data_root)
    calendar = ChinaTradingCalendar(data_root)
    stock = finder.retrieve_asset("000001.SZ")
    portal = TushareDataPortal(data_root, finder, calendar, "raw", "2024-01-08")
    assert portal.value(stock, "2024-01-02", "daily_basic.pe") == 10.0
    assert portal.fundamental(stock, "2024-01-03", "income.revenue") == 100.0
    assert len(portal.index_constituents("000300.SH", "2024-01-03")) == 2
    portal.close()


def test_qfq_hfq_and_history_are_point_in_time(data_root: Path) -> None:
    finder = AssetFinder(data_root)
    calendar = ChinaTradingCalendar(data_root)
    asset = finder.retrieve_asset("510300.SH")

    qfq = TushareDataPortal(data_root, finder, calendar, "qfq", "2024-01-08")
    hfq = TushareDataPortal(data_root, finder, calendar, "hfq", "2024-01-08")
    # Callback-visible qfq is rebased to the latest visible session, not the
    # configured backtest end, so changing a future end date cannot alter it.
    assert qfq.value(asset, "2024-01-02", "close") == pytest.approx(4.0)
    assert qfq.value(asset, "2024-01-04", "close") == pytest.approx(2.0)
    assert qfq.value(
        asset,
        "2024-01-02",
        "close",
        reference_session="2024-01-08",
    ) == pytest.approx(2.0)
    assert hfq.value(asset, "2024-01-02", "close") == pytest.approx(4.0)
    assert hfq.value(asset, "2024-01-04", "close") == pytest.approx(4.0)
    assert qfq.value(asset, "2024-01-02", "close", adjusted=False) == 4.0
    stock = finder.retrieve_asset("000001.SZ")
    raw_stock = qfq.raw_bar(stock, "2024-01-02")
    assert raw_stock.volume == pytest.approx(1_000_076.0)
    assert raw_stock.turnover == pytest.approx(10_000_000.0)

    data = BarData(qfq)
    data._set_session("2024-01-03")
    pre_action_history = data.history(asset, "close", 2)
    assert pre_action_history.tolist() == pytest.approx([4.0, 4.0])

    data._set_session("2024-01-04")
    history = data.history(asset, "close", 2)
    assert list(history.index.strftime("%Y%m%d")) == ["20240103", "20240104"]
    assert history.tolist() == pytest.approx([2.0, 2.0])

    benchmark_sessions = calendar.sessions_in_range("2024-01-02", "2024-01-08")
    benchmark = qfq.benchmark_returns("000985.CSI", benchmark_sessions)
    assert benchmark.iloc[0] == 0.0
    assert benchmark.iloc[-1] == pytest.approx(104 / 103 - 1)
    with pytest.raises(DataError, match="unavailable"):
        qfq.benchmark_returns("MISSING.INDEX", benchmark_sessions)
    qfq.close()
    hfq.close()
