"""Portfolio attribution and order-rejection table rendering."""

from __future__ import annotations

from html import escape

import numpy as np
import pandas as pd

from ..analysis.result import BacktestResult
from .formatting import REJECTION_LABELS, money, percent


def geometric_link(returns: pd.Series) -> float:
    values = pd.to_numeric(returns, errors="coerce").dropna().to_numpy(dtype=float)
    return float(np.prod(1.0 + values) - 1.0) if len(values) else 0.0


def geometric_attribution(result: BacktestResult) -> pd.Series:
    """Accumulate daily end-weight × asset-return contributions by report group."""

    positions = result.daily_positions
    required = {"record_type", "ts_code", "date", "quantity", "market_value", "weight"}
    if positions.empty or not required.issubset(positions.columns):
        return pd.Series({"CASH": 0.0}, dtype=float)
    held = positions[
        positions["record_type"].eq("POSITION")
        & pd.to_numeric(positions["quantity"], errors="coerce").gt(0)
        & pd.to_numeric(positions["market_value"], errors="coerce").gt(0)
    ].copy()
    if held.empty:
        return pd.Series({"CASH": 0.0}, dtype=float)
    held["date"] = pd.to_datetime(held["date"]).dt.normalize()
    weights = pd.to_numeric(held["weight"], errors="coerce").fillna(0.0)
    if "asset_return" in held:
        asset_returns = pd.to_numeric(held["asset_return"], errors="coerce").fillna(0.0)
    elif "adjusted_close" in held:
        held = held.sort_values(["ts_code", "date"])
        weights = pd.to_numeric(held["weight"], errors="coerce").fillna(0.0)
        asset_returns = (
            pd.to_numeric(held["adjusted_close"], errors="coerce")
            .groupby(held["ts_code"])
            .pct_change(fill_method=None)
            .fillna(0.0)
        )
    else:
        asset_returns = pd.Series(0.0, index=held.index)
    held["weighted_contribution"] = weights * asset_returns
    daily = held.groupby(["date", "ts_code"], sort=False)["weighted_contribution"].sum()
    attribution = daily.groupby("ts_code", sort=False).sum().astype(float)
    attribution.loc["CASH"] = 0.0
    return attribution


def attribution_rows(result: BacktestResult) -> str:
    trades = result.closed_trades
    transactions = result.transactions
    positions = result.daily_positions
    holding_days = pd.Series(dtype=int)
    position_columns = {
        "record_type",
        "ts_code",
        "date",
        "quantity",
        "market_value",
    }
    if not positions.empty and position_columns.issubset(positions.columns):
        market_values = pd.to_numeric(positions["market_value"], errors="coerce")
        held = positions[
            positions["record_type"].eq("POSITION")
            & pd.to_numeric(positions["quantity"], errors="coerce").gt(0)
            & market_values.gt(0)
        ]
        holding_days = held.groupby("ts_code")["date"].nunique()
        cash = positions[positions["record_type"].eq("CASH") & market_values.gt(0)]
        if not cash.empty:
            held_dates = pd.Index(pd.to_datetime(held["date"]).dt.normalize().unique())
            cash_dates = pd.Index(pd.to_datetime(cash["date"]).dt.normalize().unique())
            holding_days.loc["CASH"] = len(cash_dates.difference(held_dates))
    if trades.empty and transactions.empty and holding_days.empty:
        return '<tr><td colspan="7" class="muted">无交易归因数据</td></tr>'
    pnl = (
        trades.groupby("ts_code")["pnl"].sum()
        if not trades.empty
        else pd.Series(dtype=float)
    )
    counts = (
        transactions.groupby("ts_code").size()
        if not transactions.empty
        else pd.Series(dtype=int)
    )
    fees = (
        transactions.groupby("ts_code")["fees"].sum()
        if not transactions.empty
        else pd.Series(dtype=float)
    )
    geometric = geometric_attribution(result)
    codes = (
        pnl.index.union(counts.index)
        .union(fees.index)
        .union(holding_days.index)
        .union(geometric.index)
    )
    total_pnl = float(pnl.sum())
    ordered_codes = sorted(
        codes,
        key=lambda item: (item == "CASH", -float(pnl.get(item, 0.0))),
    )
    rows = []
    for code in ordered_codes:
        value = float(pnl.get(code, 0.0))
        contribution = value / total_pnl if total_pnl else np.nan
        rows.append(
            "<tr>"
            f"<td>{escape(str(code))}</td>"
            f"<td>{int(counts.get(code, 0))}</td>"
            f"<td>{int(holding_days.get(code, 0))}</td>"
            f"<td>{money(value)}</td>"
            f"<td>{percent(contribution)}</td>"
            f"<td>{percent(geometric.get(code, 0.0))}</td>"
            f"<td>{money(fees.get(code, 0.0))}</td>"
            "</tr>"
        )
    return "".join(rows)


def rejection_rows(result: BacktestResult) -> str:
    if result.orders.empty:
        return '<tr><td colspan="2" class="muted">无订单</td></tr>'
    rejected = result.orders[result.orders["reject_reason"].astype(str) != ""]
    if rejected.empty:
        return '<tr><td colspan="2" class="muted">无拒单或撤单</td></tr>'
    counts = rejected.groupby("reject_reason").size().sort_values(ascending=False)
    return "".join(
        f"<tr><td>{escape(REJECTION_LABELS.get(str(reason), str(reason)))}</td><td>{int(count)}</td></tr>"
        for reason, count in counts.items()
    )


# Private compatibility names historically imported from ``tualpha.reporting``.
_geometric_link = geometric_link
_geometric_attribution = geometric_attribution
_attribution_rows = attribution_rows
_rejection_rows = rejection_rows
