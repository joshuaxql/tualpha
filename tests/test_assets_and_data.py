from __future__ import annotations

import json
import shutil
from pathlib import Path

import duckdb
import pandas as pd
import pytest

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


def test_schema_v4_bundle_still_supports_non_constituent_reads(
    data_root: Path, tmp_path: Path
) -> None:
    legacy_root = tmp_path / "legacy"
    legacy_path = legacy_root / "bundles" / "tualpha"
    shutil.copytree(latest_bundle_path(data_root), legacy_path)
    (legacy_path / "index_constituents.sqlite").unlink()
    manifest_path = legacy_path / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["schema_version"] = 4
    for key in list(manifest):
        if key.startswith("index_constituent"):
            del manifest[key]
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    finder = AssetFinder(legacy_root)
    calendar = ChinaTradingCalendar(legacy_root)
    stock = finder.retrieve_asset("000001.SZ")
    portal = TushareDataPortal(legacy_root, finder, calendar, "raw", "2024-01-08")
    assert portal.value(stock, "2024-01-02", "close") == 10.0
    with pytest.raises(DataError, match="does not contain PIT index constituents"):
        portal.index_constituents("000300.SH", "2024-01-03")
    portal.close()


def test_portal_rejects_mixed_bundle_generations(data_root: Path) -> None:
    finder = AssetFinder(data_root)
    calendar = ChinaTradingCalendar(data_root)
    finder.bundle_generation = "stale-generation"
    with pytest.raises(DataError, match="different generations"):
        TushareDataPortal(data_root, finder, calendar, "raw", "2024-01-08")


def test_runtime_does_not_open_duckdb(
    data_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fail(*args: object, **kwargs: object) -> None:
        raise AssertionError("runtime must not connect to DuckDB")

    monkeypatch.setattr(duckdb, "connect", fail)
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
    with pytest.raises(DataError, match="fewer than two"):
        qfq.benchmark_returns("MISSING.INDEX", benchmark_sessions)
    qfq.close()
    hfq.close()
