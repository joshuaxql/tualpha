"""Shared report labels and scalar formatters."""

from __future__ import annotations

from html import escape
from typing import Any

import numpy as np

COLORS = {
    "primary": "#2c3e50",
    "accent": "#3498db",
    "success": "#27ae60",
    "danger": "#c0392b",
    "muted": "#7f8c8d",
    "grid": "#e8ecf1",
}
MONTHS = [
    "1月",
    "2月",
    "3月",
    "4月",
    "5月",
    "6月",
    "7月",
    "8月",
    "9月",
    "10月",
    "11月",
    "12月",
]
REJECTION_LABELS = {
    "limit_up": "涨停禁止买入",
    "limit_down": "跌停禁止卖出",
    "suspended": "停牌",
    "no_market_data": "无行情",
    "zero_volume": "零成交量",
    "invalid_lot": "交易单位不合法",
    "below_minimum_order": "不足最小交易单位",
    "t_plus_one": "T+1 可卖不足",
    "insufficient_cash": "现金不足",
    "insufficient_position": "持仓不足",
    "asset_not_alive": "未上市/已退市",
    "end_of_backtest": "回测结束",
    "zero_amount": "零数量",
}


def finite(value: Any) -> bool:
    try:
        return bool(np.isfinite(float(value)))
    except (TypeError, ValueError):
        return False


def percent(value: Any) -> str:
    return f"{float(value):.2%}" if finite(value) else "--"


def number(value: Any, digits: int = 2) -> str:
    return f"{float(value):.{digits}f}" if finite(value) else "--"


def money(value: Any) -> str:
    if not finite(value):
        return "--"
    amount = float(value)
    absolute = abs(amount)
    if absolute >= 100_000_000:
        return f"{amount / 100_000_000:.2f}亿"
    if absolute >= 10_000:
        return f"{amount / 10_000:.2f}万"
    return f"{amount:,.2f}"


def metric_card(label: str, value: str, css_class: str = "") -> str:
    return (
        '<div class="metric-card">'
        f'<div class="metric-value {css_class}">{escape(value)}</div>'
        f'<div class="metric-label">{escape(label)}</div>'
        "</div>"
    )
