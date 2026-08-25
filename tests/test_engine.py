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
    order_target_many,
    order_target_percent,
    order_target_value,
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


def test_batch_target_orders_preserve_mapping_order_and_market_rules(
    data_root: Path,
) -> None:
    def initialize(context):
        context.first = symbol("000001.SZ")
        context.second = symbol("688001.SH")
        context.day = 0
        context.submitted = []

    def handle_data(context, data):
        context.day += 1
        if context.day == 1:
            context.submitted = order_target_many(
                {context.second: 200, context.first: 100}
            )

    result = run_algorithm(
        start="2024-01-02",
        end="2024-01-03",
        initialize=initialize,
        handle_data=handle_data,
        capital_base=100_000,
        bundle_root=data_root,
        execution_time="open",
        generate_report=False,
        show_progress=False,
    )

    assert result.orders["ts_code"].tolist() == ["688001.SH", "000001.SZ"]
    assert result.orders["amount"].tolist() == [200.0, 100.0]
    assert result.orders["status"].tolist() == ["filled", "filled"]
    assert result.transactions["ts_code"].tolist() == ["688001.SH", "000001.SZ"]


def test_batch_target_uses_d1_cash_for_partial_fill(data_root: Path) -> None:
    def initialize(context):
        context.asset = symbol("000001.SZ")
        context.day = 0

    def handle_data(context, data):
        context.day += 1
        if context.day == 1:
            order_target_many({context.asset: 200})

    result = run_algorithm(
        start="2024-01-02",
        end="2024-01-03",
        initialize=initialize,
        handle_data=handle_data,
        capital_base=2_000,
        bundle_root=data_root,
        execution_time="open",
        generate_report=False,
        show_progress=False,
    )

    order_row = result.orders.iloc[0]
    assert order_row["amount"] == 200.0
    assert order_row["filled"] == 100.0
    assert order_row["status"] == "partially_filled"
    assert order_row["reject_reason"] == ""
    assert result.transactions.iloc[0]["price"] == 10.5


def test_batch_target_cancels_when_minimum_lot_is_unaffordable(
    data_root: Path,
) -> None:
    def initialize(context):
        context.asset = symbol("000001.SZ")
        context.day = 0

    def handle_data(context, data):
        context.day += 1
        if context.day == 1:
            order_target_many({context.asset: 100})

    result = run_algorithm(
        start="2024-01-02",
        end="2024-01-03",
        initialize=initialize,
        handle_data=handle_data,
        capital_base=1_000,
        bundle_root=data_root,
        execution_time="open",
        generate_report=False,
        show_progress=False,
    )

    order_row = result.orders.iloc[0]
    assert order_row["filled"] == 0.0
    assert order_row["status"] == "canceled"
    assert order_row["reject_reason"] == ""
    assert "实际现金不足" in order_row["message"]
    assert result.transactions.empty


@pytest.mark.parametrize("target_api", ["value", "percent"])
def test_single_value_targets_use_d1_cash_for_partial_fill(
    data_root: Path, target_api: str
) -> None:
    def initialize(context):
        context.asset = symbol("000001.SZ")
        context.day = 0

    def handle_data(context, data):
        context.day += 1
        if context.day != 1:
            return
        if target_api == "value":
            order_target_value(context.asset, 2_000)
        else:
            order_target_percent(context.asset, 1.0)

    result = run_algorithm(
        start="2024-01-02",
        end="2024-01-03",
        initialize=initialize,
        handle_data=handle_data,
        capital_base=2_000,
        bundle_root=data_root,
        execution_time="open",
        generate_report=False,
        show_progress=False,
    )

    order_row = result.orders.iloc[0]
    assert order_row["amount"] == 200.0
    assert order_row["filled"] == 100.0
    assert order_row["status"] == "partially_filled"
    assert order_row["reject_reason"] == ""


def test_single_target_value_cancels_when_minimum_lot_is_unaffordable(
    data_root: Path,
) -> None:
    def initialize(context):
        context.asset = symbol("000001.SZ")
        context.day = 0

    def handle_data(context, data):
        context.day += 1
        if context.day == 1:
            order_target_value(context.asset, 1_000)

    result = run_algorithm(
        start="2024-01-02",
        end="2024-01-03",
        initialize=initialize,
        handle_data=handle_data,
        capital_base=1_000,
        bundle_root=data_root,
        execution_time="open",
        generate_report=False,
        show_progress=False,
    )

    order_row = result.orders.iloc[0]
    assert order_row["status"] == "canceled"
    assert order_row["reject_reason"] == ""
    assert result.transactions.empty


def test_plain_quantity_order_keeps_insufficient_cash_rejection(
    data_root: Path,
) -> None:
    initialize, handle_data = _single_order_strategy("000001.SZ")
    result = run_algorithm(
        start="2024-01-02",
        end="2024-01-03",
        initialize=initialize,
        handle_data=handle_data,
        capital_base=1_000,
        bundle_root=data_root,
        execution_time="open",
        generate_report=False,
        show_progress=False,
    )

    order_row = result.orders.iloc[0]
    assert order_row["status"] == "rejected"
    assert order_row["reject_reason"] == "insufficient_cash"


def test_plain_target_quantity_keeps_insufficient_cash_rejection(
    data_root: Path,
) -> None:
    def initialize(context):
        context.asset = symbol("000001.SZ")
        context.day = 0

    def handle_data(context, data):
        context.day += 1
        if context.day == 1:
            order_target(context.asset, 100)

    result = run_algorithm(
        start="2024-01-02",
        end="2024-01-03",
        initialize=initialize,
        handle_data=handle_data,
        capital_base=1_000,
        bundle_root=data_root,
        execution_time="open",
        generate_report=False,
        show_progress=False,
    )

    order_row = result.orders.iloc[0]
    assert order_row["status"] == "rejected"
    assert order_row["reject_reason"] == "insufficient_cash"


def test_batch_target_position_limit_cancels_only_excess_new_assets(
    data_root: Path,
) -> None:
    def initialize(context):
        context.first = symbol("000001.SZ")
        context.second = symbol("688001.SH")
        context.day = 0

    def handle_data(context, data):
        context.day += 1
        if context.day == 1:
            order_target_many(
                {context.second: 200, context.first: 100}, position_limit=1
            )

    result = run_algorithm(
        start="2024-01-02",
        end="2024-01-03",
        initialize=initialize,
        handle_data=handle_data,
        capital_base=100_000,
        bundle_root=data_root,
        execution_time="open",
        generate_report=False,
        show_progress=False,
    )

    assert result.orders["status"].tolist() == ["filled", "canceled"]
    assert result.orders["reject_reason"].tolist() == ["", ""]
    assert result.transactions["ts_code"].tolist() == ["688001.SH"]
    assert result.metrics["open_positions"] == 1


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


def test_process_orders_uses_pending_index_not_historical_order_scan(
    data_root: Path,
) -> None:
    class NonIterableHistory(list):
        def __iter__(self):
            raise AssertionError("historical orders must not be scanned during fills")

    finder = AssetFinder(data_root)
    calendar = ChinaTradingCalendar(data_root)
    portal = TushareDataPortal(data_root, finder, calendar, "raw", "2024-01-08")
    broker = SimulationBroker(
        Portfolio(100_000),
        calendar,
        portal,
        ChinaFeeModel(),
        ChinaMarketRules(),
        "open",
    )
    session = pd.Timestamp("2024-01-02")
    stock = finder.retrieve_asset("000001.SZ")
    invalid = broker.submit(stock, 0, session, session)
    assert invalid not in broker.get_open_orders()
    order_object = broker.submit(stock, 100, session, session)
    broker.orders = NonIterableHistory(broker.orders)
    broker.process_orders(session)
    assert order_object.status is OrderStatus.FILLED
    assert broker.get_open_orders() == []
    portal.close()


def test_engine_marks_portfolio_once_per_session(data_root: Path) -> None:
    algorithm = TradingAlgorithm(
        BacktestConfig(
            start="2024-01-02",
            end="2024-01-04",
            bundle_root=data_root,
            generate_report=False,
            show_progress=False,
        )
    )
    calls = 0
    original = algorithm.broker.mark_to_market

    def counted(session: pd.Timestamp) -> None:
        nonlocal calls
        calls += 1
        original(session)

    algorithm.broker.mark_to_market = counted
    result = algorithm.run()
    assert calls == len(result.performance)


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
    assert "交易分析 (Trade Analysis)" not in report
    assert "Trade PnL Distribution" not in report
    assert "费用与交易限制 (Fees & Trading Constraints)" in report
    assert "组合归因 (Attribution)" in report
    assert "<th>持有总天数</th>" in report
    assert "<th>几何贡献收益（每日权重贡献累计）</th>" in report
    assert "贡献收益按每日结算持仓逐项计算" not in report
    assert "1%×30%−1%×10%=0.20%" not in report
    assert "<tr><td>000001.SZ</td><td>2</td><td>1</td>" in report
    assert "plotly" in report.lower()
    assert result.positions_path.read_bytes().startswith(b"\xef\xbb\xbf")
    positions = pd.read_csv(result.positions_path, encoding="utf-8-sig")
    assert set(positions["date"]) == set(
        pd.date_range("2024-01-02", "2024-01-08", freq="B").strftime("%Y-%m-%d")
    )
    assert {
        "quantity",
        "sellable_quantity",
        "raw_close",
        "adjusted_close",
        "asset_return",
    }.issubset(positions.columns)
    cash_dates = set(
        positions.loc[
            positions["record_type"].eq("CASH")
            & pd.to_numeric(positions["market_value"], errors="coerce").gt(0),
            "date",
        ]
    )
    invested_dates = set(
        positions.loc[
            positions["record_type"].eq("POSITION")
            & pd.to_numeric(positions["market_value"], errors="coerce").gt(0),
            "date",
        ]
    )
    cash_only_days = len(cash_dates - invested_dates)
    assert cash_only_days < len(cash_dates)
    assert f"<tr><td>CASH</td><td>0</td><td>{cash_only_days}</td>" in report
