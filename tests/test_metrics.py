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
            "handling_fee": 0.0,
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
