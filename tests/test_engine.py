from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
from filelock import FileLock, Timeout

from tualpha import (
    BacktestConfig,
    TradingAlgorithm,
    order,
    order_target,
    run_algorithm,
    symbol,
)
from tualpha.assets import AssetFinder
from tualpha.broker import SimulationBroker
from tualpha.bundle import bundle_lock_path
from tualpha.calendar import ChinaTradingCalendar
from tualpha.costs import ChinaFeeModel
from tualpha.data import TushareDataPortal
from tualpha.models import OrderStatus, Portfolio, RejectReason
from tualpha.rules import ChinaMarketRules


def _single_order_strategy(code: str, submit_day: int = 1):
    def initialize(context):
        context.asset = symbol(code)
        context.day = 0

    def handle_data(context, data):
        context.day += 1
        if context.day == submit_day:
            order(context.asset, 100)

    return initialize, handle_data


def test_data_portal_closes_when_strategy_raises(data_root: Path) -> None:
    def fail(context, data):
        raise RuntimeError("strategy failure")

    algorithm = TradingAlgorithm(
        BacktestConfig(
            start="2024-01-02",
            end="2024-01-03",
            bundle_root=data_root,
            generate_report=False,
        ),
        handle_data=fail,
    )
    competing = FileLock(str(bundle_lock_path(data_root)))
    with pytest.raises(Timeout):
        competing.acquire(timeout=0)
    with pytest.raises(RuntimeError, match="strategy failure"):
        algorithm.run()
    assert algorithm.data_portal._finance is None
    competing.acquire(timeout=0)
    competing.release()


def test_run_algorithm_shows_and_can_hide_progress(
    data_root: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    common = {
        "start": "2024-01-02",
        "end": "2024-01-03",
        "bundle_root": data_root,
        "generate_report": False,
        "strategy_name": "进度测试",
    }

    run_algorithm(**common)
    progress_output = capsys.readouterr().err
    assert "TuAlpha 回测：进度测试" in progress_output
    assert "100%" in progress_output
    assert "2024-01-03" in progress_output

    run_algorithm(**common, show_progress=False)
    assert capsys.readouterr().err == ""


def test_open_and_close_execution_check_only_selected_endpoint(data_root: Path) -> None:
    initialize, handle_data = _single_order_strategy("000001.SZ")
    open_result = run_algorithm(
        start="2024-01-02",
        end="2024-01-03",
        initialize=initialize,
        handle_data=handle_data,
        capital_base=100_000,
        bundle_root=data_root,
        execution_time="open",
        generate_report=False,
    )
    assert open_result.orders.iloc[0]["status"] == "filled"
    assert open_result.transactions.iloc[0]["price"] == 10.5

    close_result = run_algorithm(
        start="2024-01-02",
        end="2024-01-03",
        initialize=initialize,
        handle_data=handle_data,
        capital_base=100_000,
        bundle_root=data_root,
        execution_time="close",
        generate_report=False,
    )
    assert close_result.orders.iloc[0]["status"] == "rejected"
    assert close_result.orders.iloc[0]["reject_reason"] == "limit_up"
    assert close_result.transactions.empty


def test_open_limit_and_suspension_rejections(data_root: Path) -> None:
    initialize, handle_data = _single_order_strategy("000001.SZ", submit_day=2)
    limit_result = run_algorithm(
        start="2024-01-02",
        end="2024-01-04",
        initialize=initialize,
        handle_data=handle_data,
        capital_base=100_000,
        bundle_root=data_root,
        execution_time="open",
        generate_report=False,
    )
    assert limit_result.orders.iloc[0]["reject_reason"] == "limit_up"

    initialize, handle_data = _single_order_strategy("000001.SZ", submit_day=3)
    suspended_result = run_algorithm(
        start="2024-01-02",
        end="2024-01-05",
        initialize=initialize,
        handle_data=handle_data,
        capital_base=100_000,
        bundle_root=data_root,
        execution_time="open",
        generate_report=False,
    )
    assert suspended_result.orders.iloc[0]["reject_reason"] == "suspended"


def test_stock_and_etf_are_both_t1(data_root: Path) -> None:
    finder = AssetFinder(data_root)
    calendar = ChinaTradingCalendar(data_root)
    portal = TushareDataPortal(data_root, finder, calendar, "qfq", "2024-01-08")
    session = pd.Timestamp("2024-01-02")

    stock_broker = SimulationBroker(
        Portfolio(100_000),
        calendar,
        portal,
        ChinaFeeModel(),
        ChinaMarketRules(),
        "open",
    )
    stock = finder.retrieve_asset("000001.SZ")
    invalid = stock_broker.submit(stock, float("nan"), session, session)
    assert invalid.status is OrderStatus.REJECTED
    assert invalid.reject_reason is RejectReason.INVALID_LOT

    stock_broker.submit(stock, 100, session, session)
    stock_broker.process_orders(session)
    sell = stock_broker.submit(stock, -100, session, session)
    stock_broker.process_orders(session)
    assert sell.status is OrderStatus.REJECTED
    assert sell.reject_reason is RejectReason.T_PLUS_ONE

    etf_broker = SimulationBroker(
        Portfolio(100_000),
        calendar,
        portal,
        ChinaFeeModel(),
        ChinaMarketRules(),
        "open",
    )
    qdii = finder.retrieve_asset("513100.SH")
    etf_broker.submit(qdii, 100, session, session)
    etf_broker.process_orders(session)
    etf_sell = etf_broker.submit(qdii, -100, session, session)
    etf_broker.process_orders(session)
    assert etf_sell.status is OrderStatus.REJECTED
    assert etf_sell.reject_reason is RejectReason.T_PLUS_ONE
    portal.close()


def test_adjustment_factor_reinvests_held_position(data_root: Path) -> None:
    def initialize(context):
        context.asset = symbol("510300.SH")
        context.day = 0

    def handle_data(context, data):
        context.day += 1
        if context.day == 1:
            order(context.asset, 100)

    result = run_algorithm(
        start="2024-01-02",
        end="2024-01-04",
        initialize=initialize,
        handle_data=handle_data,
        capital_base=100_000,
        bundle_root=data_root,
        adjustment="qfq",
        execution_time="open",
        generate_report=False,
    )
    position = result.daily_positions[
        (result.daily_positions["date"] == pd.Timestamp("2024-01-04"))
        & (result.daily_positions["ts_code"] == "510300.SH")
    ].iloc[0]
    assert position["quantity"] == 200
    assert position["market_value"] == 400


def test_report_and_daily_positions_are_exported(
    data_root: Path, tmp_path: Path
) -> None:
    def initialize(context):
        context.asset = symbol("000001.SZ")
        context.day = 0

    def handle_data(context, data):
        context.day += 1
        if context.day == 1:
            order(context.asset, 100)
        elif context.day == 2:
            order_target(context.asset, 0)

    result = run_algorithm(
        start="2024-01-02",
        end="2024-01-08",
        initialize=initialize,
        handle_data=handle_data,
        capital_base=100_000,
        bundle_root=data_root,
        execution_time="open",
        output_dir=tmp_path,
        benchmark="000985.CSI",
        plotly_js="cdn",
    )
    assert result.report_path == tmp_path / "report.html"
    assert result.positions_path == tmp_path / "daily_positions.csv"
    report = result.report_path.read_text(encoding="utf-8")
    assert "核心指标 (Key Metrics)" in report
    assert "费用与交易限制 (Fees & Trading Constraints)" in report
    assert "组合归因 (Attribution)" in report
    assert "<th>持有总天数</th>" in report
    assert "<tr><td>000001.SZ</td><td>2</td><td>1</td>" in report
    assert "plotly" in report.lower()
    assert result.positions_path.read_bytes().startswith(b"\xef\xbb\xbf")
    positions = pd.read_csv(result.positions_path, encoding="utf-8-sig")
    assert set(positions["date"]) == set(
        pd.date_range("2024-01-02", "2024-01-08", freq="B").strftime("%Y-%m-%d")
    )
    assert {"quantity", "sellable_quantity", "raw_close", "adjusted_close"}.issubset(
        positions.columns
    )
