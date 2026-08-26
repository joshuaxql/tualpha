"""Programmatic backtest entry point."""

from .core.algorithm import TradingAlgorithm, run_algorithm

__all__ = ["TradingAlgorithm", "run_algorithm"]
