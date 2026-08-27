"""Backtest configuration and framework enums."""

from __future__ import annotations

import os
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

import pandas as pd

from .exceptions import ConfigurationError

DEFAULT_BUNDLE_ROOT = Path(
    os.environ.get("TUALPHA_BUNDLE_ROOT", "~/.tualpha")
).expanduser()


class AdjustmentMode(StrEnum):
    """Price representation exposed to a strategy."""

    RAW = "raw"
    QFQ = "qfq"
    HFQ = "hfq"


class ExecutionTime(StrEnum):
    """Daily bar endpoint used to execute orders from the prior session."""

    OPEN = "open"
    CLOSE = "close"


class PlotlyJsMode(StrEnum):
    """How Plotly JavaScript is included in an HTML report."""

    INLINE = "inline"
    CDN = "cdn"


def normalize_session(value: str | pd.Timestamp) -> pd.Timestamp:
    """Convert a date-like value to a timezone-naive normalized timestamp."""

    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is not None:
        timestamp = timestamp.tz_convert("Asia/Shanghai").tz_localize(None)
    return timestamp.normalize()


@dataclass(slots=True)
class BacktestConfig:
    """Configuration for one daily backtest run."""

    start: str | pd.Timestamp
    end: str | pd.Timestamp
    capital_base: float = 1_000_000.0
    bundle_root: str | Path = DEFAULT_BUNDLE_ROOT
    adjustment: AdjustmentMode | str = AdjustmentMode.QFQ
    execution_time: ExecutionTime | str = ExecutionTime.OPEN
    output_dir: str | Path | None = None
    benchmark: str | None = None
    strategy_name: str = "TuAlpha 回测策略"
    generate_report: bool = True
    show_progress: bool = True
    plotly_js: PlotlyJsMode | str = PlotlyJsMode.INLINE
    annualization_factor: int = 252
    bundle_name: str = "tualpha"
    column_cache_mib: int | None = None

    def __post_init__(self) -> None:
        self.start = normalize_session(self.start)
        self.end = normalize_session(self.end)
        self.bundle_root = Path(self.bundle_root).expanduser()
        self.output_dir = Path(self.output_dir) if self.output_dir is not None else None
        try:
            self.adjustment = AdjustmentMode(self.adjustment)
            self.execution_time = ExecutionTime(self.execution_time)
            self.plotly_js = PlotlyJsMode(self.plotly_js)
        except ValueError as exc:
            raise ConfigurationError(str(exc)) from exc

        if self.start > self.end:
            raise ConfigurationError("start must not be after end")
        if self.capital_base <= 0:
            raise ConfigurationError("capital_base must be positive")
        if self.annualization_factor <= 0:
            raise ConfigurationError("annualization_factor must be positive")
        if not self.bundle_name.strip():
            raise ConfigurationError("bundle_name must not be empty")
        if self.column_cache_mib is not None and self.column_cache_mib < 0:
            raise ConfigurationError("column_cache_mib must not be negative")
