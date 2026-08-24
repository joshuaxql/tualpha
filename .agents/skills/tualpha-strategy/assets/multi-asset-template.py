"""TuAlpha 多因子月度选股模板。

示例仅展示正确的数据时点和 API 用法，不代表投资建议。请替换候选池、
因子、权重和风险规则。
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from tualpha import order_target_percent, record, run_algorithm, symbol

CANDIDATE_CODES = [
    "000001.SZ",
    "000002.SZ",
    "600000.SH",
    "600036.SH",
    "601318.SH",
]
LOOKBACK = 60
HOLD_COUNT = 3
TOTAL_EXPOSURE = 0.90


def initialize(context):
    context.assets = [symbol(code) for code in CANDIDATE_CODES]
    context.last_rebalance_month = None


def _factor_row(asset, data):
    if not data.can_trade(asset):
        return None
    if data.current(asset, "stock_st.is_st") == 1:
        return None

    closes = data.history(asset, "close", LOOKBACK).dropna()
    if len(closes) < LOOKBACK or closes.iloc[0] <= 0:
        return None

    momentum = float(closes.iloc[-1] / closes.iloc[0] - 1.0)
    pe_ttm = data.current(asset, "daily_basic.pe_ttm")
    roe = data.fundamental(asset, "fina_indicator.roe")
    if not all(np.isfinite(value) for value in (momentum, pe_ttm, roe)):
        return None
    if pe_ttm <= 0:
        return None
    return {
        "asset": asset,
        "momentum": momentum,
        "pe_ttm": float(pe_ttm),
        "roe": float(roe),
    }


def handle_data(context, data):
    month = (context.datetime.year, context.datetime.month)
    if month == context.last_rebalance_month:
        return

    rows = [
        row for asset in context.assets if (row := _factor_row(asset, data)) is not None
    ]
    factors = pd.DataFrame(rows)
    selected = []
    if not factors.empty:
        # 横截面百分位排名：动量和 ROE 越高越好，PE 越低越好。
        factors["score"] = (
            factors["momentum"].rank(pct=True)
            + factors["roe"].rank(pct=True)
            + (-factors["pe_ttm"]).rank(pct=True)
        )
        selected = factors.nlargest(HOLD_COUNT, "score")["asset"].tolist()

    target_weight = TOTAL_EXPOSURE / len(selected) if selected else 0.0
    selected_set = set(selected)
    for asset in context.assets:
        # 无法交易的现有持仓会保留；订单仍可能在 D+1 因市场状态变化被拒绝。
        if data.can_trade(asset):
            order_target_percent(
                asset,
                target_weight if asset in selected_set else 0.0,
            )

    context.last_rebalance_month = month
    record(
        candidate_count=len(factors),
        selected_count=len(selected),
        target_weight=target_weight,
    )


if __name__ == "__main__":
    result = run_algorithm(
        start="2020-01-01",
        end="2025-12-31",
        initialize=initialize,
        handle_data=handle_data,
        capital_base=1_000_000,
        adjustment="qfq",
        execution_time="open",
        benchmark="000300.SH",
        output_dir="outputs/multi_factor_strategy",
        strategy_name="多因子月度选股策略",
    )
    print(result.summary())
