from __future__ import annotations

from dataclasses import replace

import pandas as pd
import pytest

from tualpha.assets import Asset, AssetType, Board
from tualpha.broker import SimulationBroker
from tualpha.costs import ChinaFeeModel
from tualpha.data import DailyBar
from tualpha.models import OrderStatus, Portfolio, Position, RejectReason
from tualpha.rules import ChinaMarketRules


def _asset(code: str, board: Board, asset_type: AssetType = AssetType.STOCK) -> Asset:
    return Asset(
        sid=1,
        ts_code=code,
        symbol=code[:6],
        name="test",
        asset_type=asset_type,
        exchange="SSE",
        board=board,
        price_tick=0.001 if asset_type is AssetType.ETF else 0.01,
    )


def test_board_lot_rules() -> None:
    rules = ChinaMarketRules()
    main = _asset("000001.SZ", Board.MAIN)
    star = _asset("688001.SH", Board.STAR)
    bse = _asset("920001.BJ", Board.BSE)

    assert rules.normalize_buy(main, 299) == 200
    assert rules.validate_quantity(main, 150, is_buy=True) == (
        RejectReason.INVALID_LOT,
        "该证券委托数量必须为 100 股/份的整数倍",
    )
    assert rules.normalize_buy(star, 199) == 0
    assert rules.normalize_buy(star, 201) == 201
    assert rules.validate_quantity(star, 201, is_buy=True) is None
    assert rules.normalize_buy(bse, 101) == 101
    assert (
        rules.validate_quantity(main, 53.5, is_buy=False, position_amount=53.5) is None
    )


def test_directional_price_limit_rules() -> None:
    rules = ChinaMarketRules()
    asset = _asset("000001.SZ", Board.MAIN)
    bar = DailyBar(
        asset=asset,
        session=pd.Timestamp("2024-01-02"),
        open=11.0,
        high=11.0,
        low=9.0,
        close=9.0,
        pre_close=10.0,
        volume=1000,
        turnover=10000,
        up_limit=11.0,
        down_limit=9.0,
    )
    assert (
        rules.validate_market(asset, bar, is_buy=True, execution_field="open")[0]
        is RejectReason.LIMIT_UP
    )
    assert (
        rules.validate_market(asset, bar, is_buy=False, execution_field="close")[0]
        is RejectReason.LIMIT_DOWN
    )
    assert (
        rules.validate_market(asset, bar, is_buy=False, execution_field="open") is None
    )
    assert (
        rules.validate_market(asset, bar, is_buy=True, execution_field="close") is None
    )


def test_position_totals_cache_is_invalidated_by_lot_changes() -> None:
    asset = _asset("000001.SZ", Board.MAIN)
    position = Position(asset=asset)
    acquired = pd.Timestamp("2024-01-02")
    settled = pd.Timestamp("2024-01-03")
    position.add_lot(100, 10.0, acquired, settled)
    assert position.amount == 100
    assert position.total_cost == 1000
    position.add_lot(200, 12.0, acquired, settled)
    assert position.amount == 300
    assert position.total_cost == 3400
    position.consume(150, settled)
    assert position.amount == 150
    assert position.total_cost == 1800
    position.apply_adjustment(2.0)
    assert position.amount == 300
    assert position.total_cost == 1800


@pytest.mark.parametrize(
    ("position_amount", "adjustment_ratio"),
    [(40_900.0, 0.999892212346), (381.847195, 1.000669)],
)
def test_corporate_actions_keep_pending_full_liquidation_synchronized(
    position_amount: float,
    adjustment_ratio: float,
) -> None:
    asset = _asset("000001.SZ", Board.MAIN)
    previous_session = pd.Timestamp("2024-01-02")
    session = pd.Timestamp("2024-01-03")
    portfolio = Portfolio(1_000_000)
    position = Position(asset=asset, last_sale_price=10.0)
    position.add_lot(position_amount, 10.0, previous_session, previous_session)
    portfolio.positions[asset] = position
    bar = DailyBar(
        asset=asset,
        session=session,
        open=10.0,
        high=10.0,
        low=10.0,
        close=10.0,
        pre_close=10.0,
        volume=1_000_000,
        turnover=10_000_000,
        up_limit=11.0,
        down_limit=9.0,
    )

    class Portal:
        @staticmethod
        def factor(requested_asset, requested_session):
            assert requested_asset == asset
            return adjustment_ratio if requested_session == session else 1.0

        @staticmethod
        def execution_bars(assets, requested_session, execution_field):
            assert assets == [asset]
            assert requested_session == session
            assert execution_field == "open"
            return {asset: bar}

    broker = SimulationBroker(
        portfolio=portfolio,
        calendar=None,
        data_portal=Portal(),
        fee_model=ChinaFeeModel(),
        market_rules=ChinaMarketRules(),
        execution_field="open",
    )
    order = broker.submit(asset, -position_amount, previous_session, session)

    broker.apply_corporate_actions(previous_session, session)

    adjusted_amount = position_amount * adjustment_ratio
    assert portfolio.amount(asset) == pytest.approx(adjusted_amount)
    assert order.remaining == pytest.approx(-adjusted_amount)
    fills = broker.process_orders(session)
    assert len(fills) == 1
    assert fills[0].amount == pytest.approx(-adjusted_amount)
    assert order.status is OrderStatus.FILLED
    assert order.reject_reason is None
    assert portfolio.amount(asset) == 0.0


def test_corporate_actions_do_not_rewrite_non_liquidation_orders() -> None:
    asset = _asset("000001.SZ", Board.MAIN)
    previous_session = pd.Timestamp("2024-01-02")
    session = pd.Timestamp("2024-01-03")
    portfolio = Portfolio(1_000_000)
    position = Position(asset=asset, last_sale_price=10.0)
    position.add_lot(1_000, 10.0, previous_session, previous_session)
    portfolio.positions[asset] = position

    class Portal:
        @staticmethod
        def factor(asset, requested_session):
            return 2.0 if requested_session == session else 1.0

    broker = SimulationBroker(
        portfolio=portfolio,
        calendar=None,
        data_portal=Portal(),
        fee_model=ChinaFeeModel(),
        market_rules=ChinaMarketRules(),
        execution_field="open",
    )
    sell = broker.submit(asset, -500, previous_session, session)
    buy = broker.submit(asset, 500, previous_session, session)

    broker.apply_corporate_actions(previous_session, session)

    assert portfolio.amount(asset) == 2_000
    assert sell.amount == -500
    assert buy.amount == 500


def test_zero_valuation_price_does_not_reuse_stale_price() -> None:
    asset = _asset("000001.SZ", Board.MAIN)
    portfolio = Portfolio(100_000)
    position = Position(asset=asset, last_sale_price=10.0)
    position.add_lot(100, 10.0, pd.Timestamp("2024-01-02"), pd.Timestamp("2024-01-03"))
    portfolio.positions[asset] = position
    portfolio.mark_to_market({asset: 0.0})
    assert portfolio.positions_value == 0.0
    assert portfolio.portfolio_value == 100_000


def test_delisted_position_is_written_down_to_zero_without_adding_cash() -> None:
    asset = replace(
        _asset("000001.SZ", Board.MAIN),
        delist_date=pd.Timestamp("2024-01-03"),
    )
    portfolio = Portfolio(100_000)
    portfolio.cash = 99_000
    position = Position(asset=asset, last_sale_price=10.0)
    position.add_lot(100, 10.0, pd.Timestamp("2024-01-02"), pd.Timestamp("2024-01-03"))
    portfolio.positions[asset] = position

    class Portal:
        @staticmethod
        def values(assets, session, fields, *, adjusted):
            return pd.DataFrame({"close": [10.0] * len(assets)})

    broker = SimulationBroker(
        portfolio=portfolio,
        calendar=None,
        data_portal=Portal(),
        fee_model=ChinaFeeModel(),
        market_rules=ChinaMarketRules(),
        execution_field="open",
    )
    broker.mark_to_market(pd.Timestamp("2024-01-03"))
    assert portfolio.positions_value == 1_000
    assert portfolio.portfolio_value == 100_000

    cash_before_delisting = portfolio.cash
    broker.mark_to_market(pd.Timestamp("2024-01-04"))
    assert portfolio.amount(asset) == 100
    assert portfolio.positions_value == 0.0
    assert portfolio.portfolio_value == cash_before_delisting == 99_000


def test_fee_breakdown_avoids_duplicate_handling() -> None:
    model = ChinaFeeModel()
    stock = _asset("000001.SZ", Board.MAIN)
    etf = _asset("510300.SH", Board.ETF, AssetType.ETF)

    stock_sell = model.calculate(stock, 100_000, is_sell=True, session="2024-01-02")
    assert stock_sell.commission == 30.0
    assert stock_sell.stamp_tax == 50.0
    assert stock_sell.transfer_fee == 1.0
    assert stock_sell.total == 81.0

    historical = model.calculate(stock, 100_000, is_sell=True, session="2023-08-25")
    assert historical.stamp_tax == 100.0

    etf_sell = model.calculate(etf, 100_000, is_sell=True, session="2024-01-02")
    assert etf_sell.stamp_tax == 0.0
    assert etf_sell.transfer_fee == 0.0
    assert etf_sell.total == 30.0
