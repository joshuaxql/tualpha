"""Dependency-light performance and risk metrics."""

from __future__ import annotations

import math
from typing import Any

import numpy as np
import pandas as pd


def _safe_ratio(numerator: float, denominator: float) -> float:
    return (
        numerator / denominator if denominator and np.isfinite(denominator) else np.nan
    )


def calculate_metrics(
    performance: pd.DataFrame,
    closed_trades: pd.DataFrame,
    daily_positions: pd.DataFrame,
    annualization_factor: int = 252,
) -> dict[str, Any]:
    if performance.empty:
        return {}
    returns = performance["returns"].fillna(0.0).astype(float)
    portfolio = performance["portfolio_value"].astype(float)
    total_return = float(performance["algorithm_period_return"].iloc[-1])
    periods = max(1, len(returns) - 1)
    cagr = (
        (1.0 + total_return) ** (annualization_factor / periods) - 1.0
        if total_return > -1
        else -1.0
    )
    daily_std = float(returns.std(ddof=1)) if len(returns) > 1 else 0.0
    volatility = daily_std * math.sqrt(annualization_factor)
    sharpe = _safe_ratio(
        float(returns.mean()) * math.sqrt(annualization_factor), daily_std
    )
    downside_returns = np.minimum(returns.to_numpy(), 0.0)
    downside_deviation = float(np.sqrt(np.mean(np.square(downside_returns))))
    sortino = _safe_ratio(
        float(returns.mean()) * math.sqrt(annualization_factor), downside_deviation
    )
    drawdown = portfolio / portfolio.cummax() - 1.0
    max_drawdown = abs(float(drawdown.min()))
    calmar = _safe_ratio(cagr, max_drawdown)

    if closed_trades.empty:
        wins = losses = pd.Series(dtype=float)
        win_rate = np.nan
        profit_factor = np.nan
    else:
        pnl = closed_trades["pnl"].astype(float)
        wins = pnl[pnl > 0]
        losses = pnl[pnl < 0]
        win_rate = float((pnl > 0).mean())
        profit_factor = _safe_ratio(float(wins.sum()), abs(float(losses.sum())))

    open_positions = 0
    if not daily_positions.empty:
        final_date = daily_positions["date"].max()
        market_values = (
            pd.to_numeric(daily_positions["market_value"], errors="coerce")
            if "market_value" in daily_positions
            else pd.Series(0.0, index=daily_positions.index)
        )
        final_rows = daily_positions[
            (daily_positions["date"] == final_date)
            & (daily_positions["record_type"] == "POSITION")
            & market_values.gt(0)
        ]
        open_positions = len(final_rows)

    metrics: dict[str, Any] = {
        "total_return": total_return,
        "cagr": cagr,
        "sharpe": sharpe,
        "sortino": sortino,
        "calmar": calmar,
        "max_drawdown": max_drawdown,
        "volatility": volatility,
        "win_rate": win_rate,
        "profit_factor": profit_factor,
        "closed_trades": len(closed_trades),
        "open_positions": open_positions,
        "final_value": float(portfolio.iloc[-1]),
        "total_commission": float(performance["commission"].sum()),
        "total_stamp_tax": float(performance["stamp_tax"].sum()),
        "total_handling_fee": float(performance["handling_fee"].sum()),
        "total_transfer_fee": float(performance["transfer_fee"].sum()),
        "total_fees": float(performance["fees"].sum()),
        "turnover": float(performance["turnover"].sum()),
    }

    if "benchmark_returns" in performance:
        benchmark = performance["benchmark_returns"].fillna(0.0).astype(float)
        excess = returns - benchmark
        tracking_error = float(excess.std(ddof=1)) * math.sqrt(annualization_factor)
        information_ratio = _safe_ratio(
            float(excess.mean()) * annualization_factor, tracking_error
        )
        variance = float(benchmark.var(ddof=1))
        beta = _safe_ratio(float(returns.cov(benchmark)), variance)
        alpha = (
            float(returns.mean())
            - (beta if np.isfinite(beta) else 0.0) * float(benchmark.mean())
        ) * annualization_factor
        benchmark_total = float((1.0 + benchmark).prod() - 1.0)
        metrics.update(
            {
                "benchmark_total_return": benchmark_total,
                "total_excess_return": total_return - benchmark_total,
                "tracking_error": tracking_error,
                "information_ratio": information_ratio,
                "beta": beta,
                "alpha": alpha,
            }
        )
    return metrics
