from __future__ import annotations

import pandas as pd
import pytest

from tualpha.config import BacktestConfig
from tualpha.reporting import (
    _attribution_rows,
    _geometric_attribution,
    _geometric_link,
)
from tualpha.result import BacktestResult


def test_geometric_link_compounds_positive_and_negative_periods() -> None:
    returns = pd.Series([0.05, -0.03])
    assert _geometric_link(returns) == pytest.approx(1.05 * 0.97 - 1.0)


def test_geometric_attribution_sums_daily_weighted_asset_returns() -> None:
    dates = pd.date_range("2024-01-02", periods=2, freq="B")
    performance = pd.DataFrame(
        {"returns": [0.0, 0.0], "portfolio_value": [100_000.0, 100_000.0]},
        index=dates,
    )
    positions = pd.DataFrame(
        [
            {
                "date": date,
                "record_type": "CASH",
                "ts_code": "CASH",
                "quantity": 0.0,
                "market_value": cash,
                "weight": cash_weight,
                "asset_return": 0.0,
            }
            for date, cash, cash_weight in zip(
                dates, [20_000.0, 0.0], [0.2, 0.0], strict=True
            )
        ]
        + [
            {
                "date": dates[0],
                "record_type": "POSITION",
                "ts_code": "A",
                "quantity": 100,
                "market_value": 30_000,
                "weight": 0.3,
                "asset_return": 0.01,
            },
            {
                "date": dates[0],
                "record_type": "POSITION",
                "ts_code": "B",
                "quantity": 100,
                "market_value": 50_000,
                "weight": 0.5,
                "asset_return": -0.02,
            },
            {
                "date": dates[1],
                "record_type": "POSITION",
                "ts_code": "A",
                "quantity": 100,
                "market_value": 10_000,
                "weight": 0.1,
                "asset_return": -0.01,
            },
            {
                "date": dates[1],
                "record_type": "POSITION",
                "ts_code": "B",
                "quantity": 100,
                "market_value": 90_000,
                "weight": 0.9,
                "asset_return": 0.02,
            },
        ]
    )
    result = BacktestResult(
        config=BacktestConfig(start=dates[0], end=dates[-1], generate_report=False),
        performance=performance,
        daily_positions=positions,
        orders=pd.DataFrame(),
        transactions=pd.DataFrame(),
        closed_trades=pd.DataFrame(),
        metrics={},
    )

    attribution = _geometric_attribution(result)
    assert attribution["A"] == pytest.approx(0.01 * 0.3 + (-0.01) * 0.1)
    assert attribution["B"] == pytest.approx((-0.02) * 0.5 + 0.02 * 0.9)
    assert attribution["CASH"] == 0.0


def test_cash_holding_days_count_only_fully_cash_sessions() -> None:
    dates = pd.date_range("2024-01-02", periods=3, freq="B")
    positions = pd.DataFrame(
        [
            {
                "date": date,
                "record_type": "CASH",
                "ts_code": "CASH",
                "quantity": 0.0,
                "market_value": cash,
                "weight": cash / 100_000.0,
            }
            for date, cash in zip(dates, [100_000.0, 20_000.0, 100_000.0], strict=True)
        ]
        + [
            {
                "date": dates[1],
                "record_type": "POSITION",
                "ts_code": "LIVE",
                "quantity": 100.0,
                "market_value": 80_000.0,
                "weight": 0.8,
            },
            {
                "date": dates[2],
                "record_type": "POSITION",
                "ts_code": "DELISTED_AUDIT",
                "quantity": 100.0,
                "market_value": 0.0,
                "weight": 0.0,
            },
        ]
    )
    result = BacktestResult(
        config=BacktestConfig(start=dates[0], end=dates[-1], generate_report=False),
        performance=pd.DataFrame(
            {"returns": [0.0, 0.0, 0.0], "portfolio_value": [100_000.0] * 3},
            index=dates,
        ),
        daily_positions=positions,
        orders=pd.DataFrame(),
        transactions=pd.DataFrame(),
        closed_trades=pd.DataFrame(),
        metrics={},
    )

    rows = _attribution_rows(result)

    assert "<tr><td>CASH</td><td>0</td><td>2</td>" in rows
