from __future__ import annotations

import numpy as np
import pandas as pd

from tualpha.metrics import calculate_metrics


def test_sortino_uses_all_period_downside_deviation() -> None:
    index = pd.date_range("2024-01-02", periods=3, freq="B")
    returns = pd.Series([0.0, 0.01, -0.005], index=index)
    values = 100_000 * (1 + returns).cumprod()
    performance = pd.DataFrame(
        {
            "returns": returns,
            "portfolio_value": values,
            "algorithm_period_return": values / 100_000 - 1,
            "commission": 0.0,
            "stamp_tax": 0.0,
            "transfer_fee": 0.0,
            "fees": 0.0,
            "turnover": 0.0,
        },
        index=index,
    )
    positions = pd.DataFrame(
        [{"date": index[-1], "record_type": "CASH", "ts_code": "CASH"}]
    )
    metrics = calculate_metrics(performance, pd.DataFrame(), positions)
    assert np.isfinite(metrics["sortino"])


def test_open_positions_exclude_zero_value_delisted_audit_rows() -> None:
    date = pd.Timestamp("2024-01-02")
    performance = pd.DataFrame(
        {
            "returns": [0.0],
            "portfolio_value": [100_000.0],
            "algorithm_period_return": [0.0],
            "commission": [0.0],
            "stamp_tax": [0.0],
            "transfer_fee": [0.0],
            "fees": [0.0],
            "turnover": [0.0],
        },
        index=[date],
    )
    positions = pd.DataFrame(
        [
            {
                "date": date,
                "record_type": "POSITION",
                "ts_code": "LIVE",
                "market_value": 90_000.0,
            },
            {
                "date": date,
                "record_type": "POSITION",
                "ts_code": "DELISTED",
                "market_value": 0.0,
            },
            {
                "date": date,
                "record_type": "CASH",
                "ts_code": "CASH",
                "market_value": 10_000.0,
            },
        ]
    )

    metrics = calculate_metrics(performance, pd.DataFrame(), positions)

    assert metrics["open_positions"] == 1
