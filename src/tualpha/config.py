"""Compatibility facade for framework configuration."""

from .foundation.config import (
    DEFAULT_BUNDLE_ROOT,
    AdjustmentMode,
    BacktestConfig,
    ExecutionTime,
    PlotlyJsMode,
    normalize_session,
)

__all__ = [
    "DEFAULT_BUNDLE_ROOT",
    "AdjustmentMode",
    "BacktestConfig",
    "ExecutionTime",
    "PlotlyJsMode",
    "normalize_session",
]
