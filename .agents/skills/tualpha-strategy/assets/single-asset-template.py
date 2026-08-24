"""TuAlpha 单资产趋势策略模板。

复制到 examples/ 或用户指定目录后，替换资产、信号和回测参数。
"""

from __future__ import annotations

import pandas as pd

from tualpha import order_target_percent, record, run_algorithm, symbol

FAST_WINDOW = 20
SLOW_WINDOW = 60
TARGET_WEIGHT = 0.95


def initialize(context):
    context.asset = symbol("510300.SH")


def handle_data(context, data):
    closes = data.history(context.asset, "close", SLOW_WINDOW).dropna()
    if len(closes) < SLOW_WINDOW:
        record(ready=0, signal=0, target_weight=0.0)
        return

    fast = float(closes.iloc[-FAST_WINDOW:].mean())
    slow = float(closes.mean())
    signal = int(fast > slow)
    target = TARGET_WEIGHT if signal else 0.0
    amount = context.portfolio.amount(context.asset)

    # 当前可交易不代表 D+1 一定能成交，但可避免在已知停牌日反复下单。
    if data.can_trade(context.asset):
        if target > 0 and amount <= 0:
            order_target_percent(context.asset, target)
        elif target == 0 and amount > 0:
            order_target_percent(context.asset, 0.0)

    raw_close = data.raw_current(context.asset, "close")
    actual_weight = (
        amount * raw_close / context.portfolio.portfolio_value
        if pd.notna(raw_close) and context.portfolio.portfolio_value > 0
        else 0.0
    )
    record(
        ready=1,
        close=float(closes.iloc[-1]),
        fast=fast,
        slow=slow,
        signal=signal,
        target_weight=target,
        actual_weight=actual_weight,
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
        output_dir="outputs/single_asset_strategy",
        strategy_name="单资产趋势策略",
    )
    print(result.summary())
