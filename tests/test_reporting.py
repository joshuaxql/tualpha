from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from tualpha.config import BacktestConfig
from tualpha.reporting import _geometric_attribution, _geometric_link
from tualpha.result import BacktestResult


def test_geometric_link_compounds_positive_and_negative_periods() -> None:
    returns = pd.Series([0.05, -0.03])
    assert _geometric_link(returns) == pytest.approx(1.05 * 0.97 - 1.0)


def test_geometric_attribution_uses_daily_dominant_position_and_cash() -> None:
    dates = pd.date_range("2024-01-02", periods=5, freq="B")
    daily_returns = pd.Series([0.05, -0.03, 0.02, 0.01, -0.005], index=dates)
    performance = pd.DataFrame(
        {
            "returns": daily_returns,
            "portfolio_value": 100_000 * (1.0 + daily_returns).cumprod(),
        },
        index=dates,
    )
    positions = pd.DataFrame(
        [
            {
                "date": dates[0],
                "record_type": "POSITION",
                "ts_code": "000001.SZ",
                "quantity": 100,
                "market_value": 60_000,
            },
            {
                "date": dates[0],
                "record_type": "POSITION",
                "ts_code": "510300.SH",
                "quantity": 100,
                "market_value": 40_000,
            },
            {
                "date": dates[1],
                "record_type": "POSITION",
                "ts_code": "000001.SZ",
                "quantity": 100,
                "market_value": 55_000,
            },
            {
                "date": dates[1],
                "record_type": "POSITION",
                "ts_code": "510300.SH",
                "quantity": 100,
                "market_value": 45_000,
            },
            {
                "date": dates[2],
                "record_type": "POSITION",
                "ts_code": "510300.SH",
                "quantity": 100,
                "market_value": 100_000,
            },
            {
                "date": dates[3],
                "record_type": "POSITION",
                "ts_code": "000001.SZ",
                "quantity": 100,
                "market_value": 100_000,
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
    assert attribution["000001.SZ"] == pytest.approx(1.05 * 0.97 * 1.01 - 1.0)
    assert attribution["510300.SH"] == pytest.approx(0.02)
    assert attribution["CASH"] == pytest.approx(-0.005)
    linked = float(np.prod(1.0 + attribution) - 1.0)
    assert linked == pytest.approx(float(np.prod(1.0 + daily_returns) - 1.0))
