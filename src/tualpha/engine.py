"""Compatibility facade for :mod:`tualpha.core.algorithm`."""

from .core.algorithm import AlgorithmContext, TradingAlgorithm, run_algorithm

__all__ = ["AlgorithmContext", "TradingAlgorithm", "run_algorithm"]
