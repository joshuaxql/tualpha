from __future__ import annotations

import pandas as pd

from tualpha.assets import Asset, AssetType, Board
from tualpha.costs import ChinaFeeModel
from tualpha.data import DailyBar
from tualpha.models import Portfolio, Position, RejectReason
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


def test_zero_valuation_price_does_not_reuse_stale_price() -> None:
    asset = _asset("000001.SZ", Board.MAIN)
    portfolio = Portfolio(100_000)
    position = Position(asset=asset, last_sale_price=10.0)
    position.add_lot(100, 10.0, pd.Timestamp("2024-01-02"), pd.Timestamp("2024-01-03"))
    portfolio.positions[asset] = position
    portfolio.mark_to_market({asset: 0.0})
    assert portfolio.positions_value == 0.0
    assert portfolio.portfolio_value == 100_000


def test_fee_breakdown_avoids_duplicate_handling() -> None:
    model = ChinaFeeModel()
    stock = _asset("000001.SZ", Board.MAIN)
    etf = _asset("510300.SH", Board.ETF, AssetType.ETF)

    stock_sell = model.calculate(stock, 100_000, is_sell=True, session="2024-01-02")
    assert stock_sell.commission == 30.0
    assert stock_sell.stamp_tax == 50.0
    assert stock_sell.transfer_fee == 1.0
    assert stock_sell.handling_fee == 0.0
    assert stock_sell.included_handling_fee == 3.41
    assert stock_sell.total == 81.0

    historical = model.calculate(stock, 100_000, is_sell=True, session="2023-08-25")
    assert historical.stamp_tax == 100.0
    assert historical.included_handling_fee == 4.87

    etf_sell = model.calculate(etf, 100_000, is_sell=True, session="2024-01-02")
    assert etf_sell.stamp_tax == 0.0
    assert etf_sell.transfer_fee == 0.0
    assert etf_sell.total == 30.0
