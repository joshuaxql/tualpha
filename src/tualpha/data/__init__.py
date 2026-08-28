"""Point-in-time data facade, portal, calendar, and Bundle storage."""

from __future__ import annotations

from typing import Any

__all__ = [
    "BarData",
    "BundleDataPortal",
    "ChinaTradingCalendar",
    "DailyBar",
    "FactorData",
    "TushareDataPortal",
    "factor_data",
]


def __getattr__(name: str) -> Any:
    if name in {"BarData", "DailyBar"}:
        from .bar import BarData, DailyBar

        return {"BarData": BarData, "DailyBar": DailyBar}[name]
    if name in {"BundleDataPortal", "TushareDataPortal"}:
        from .portal import BundleDataPortal, TushareDataPortal

        return {
            "BundleDataPortal": BundleDataPortal,
            "TushareDataPortal": TushareDataPortal,
        }[name]
    if name == "ChinaTradingCalendar":
        from .trading_calendar import ChinaTradingCalendar

        return ChinaTradingCalendar
    if name in {"FactorData", "factor_data"}:
        from .research import FactorData, factor_data

        return {"FactorData": FactorData, "factor_data": factor_data}[name]
    raise AttributeError(name)
