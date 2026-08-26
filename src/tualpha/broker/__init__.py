"""Simulation broker, matching rules, and transaction costs."""

from __future__ import annotations

from typing import Any

__all__ = [
    "ChinaFeeModel",
    "ChinaMarketRules",
    "DailyBarMatcher",
    "RateSchedule",
    "SimulationBroker",
]


def __getattr__(name: str) -> Any:
    if name in {"ChinaFeeModel", "RateSchedule"}:
        from .costs import ChinaFeeModel, RateSchedule

        return {"ChinaFeeModel": ChinaFeeModel, "RateSchedule": RateSchedule}[name]
    if name == "ChinaMarketRules":
        from .market_rules import ChinaMarketRules

        return ChinaMarketRules
    if name == "DailyBarMatcher":
        from .matcher import DailyBarMatcher

        return DailyBarMatcher
    if name == "SimulationBroker":
        from .simulation import SimulationBroker

        return SimulationBroker
    raise AttributeError(name)
