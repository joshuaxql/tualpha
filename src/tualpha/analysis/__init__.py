"""Backtest result models and dependency-light performance analytics."""

from .metrics import calculate_metrics
from .result import BacktestResult

__all__ = ["BacktestResult", "calculate_metrics"]
