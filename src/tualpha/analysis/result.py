"""Structured backtest outputs and file export helpers."""

from __future__ import annotations

import os
import time
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from uuid import uuid4

import pandas as pd

from ..foundation.config import BacktestConfig
from ..foundation.exceptions import DataError

_PUBLISH_ATTEMPTS = 5
_PUBLISH_RETRY_SECONDS = 0.05


def _temporary_sibling(destination: Path) -> Path:
    return destination.with_name(f".{destination.name}.{uuid4().hex}.tmp")


def _publish_atomic(temporary: Path, destination: Path) -> None:
    delay = _PUBLISH_RETRY_SECONDS
    for attempt in range(_PUBLISH_ATTEMPTS):
        try:
            os.replace(temporary, destination)
            return
        except PermissionError as exc:
            if attempt + 1 == _PUBLISH_ATTEMPTS:
                raise DataError(
                    f"cannot publish backtest output {destination}; "
                    "close programs that have the file open and retry"
                ) from exc
            time.sleep(delay)
            delay *= 2.0


def _write_atomic(destination: Path, writer: Callable[[Path], Any]) -> None:
    temporary = _temporary_sibling(destination)
    try:
        writer(temporary)
        if not temporary.is_file():
            raise DataError(f"backtest output writer created no file: {temporary}")
        _publish_atomic(temporary, destination)
    finally:
        with suppress(OSError):
            temporary.unlink(missing_ok=True)


@dataclass(slots=True)
class BacktestResult:
    config: BacktestConfig
    performance: pd.DataFrame
    daily_positions: pd.DataFrame
    orders: pd.DataFrame
    transactions: pd.DataFrame
    closed_trades: pd.DataFrame
    metrics: dict[str, Any]
    records: pd.DataFrame = field(default_factory=pd.DataFrame)
    report_path: Path | None = None
    positions_path: Path | None = None

    def export_positions(self, path: str | Path) -> Path:
        destination = Path(path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        _write_atomic(
            destination,
            lambda temporary: self.daily_positions.to_csv(
                temporary,
                index=False,
                encoding="utf-8-sig",
                float_format="%.8f",
            ),
        )
        self.positions_path = destination
        return destination

    def export_report(self, path: str | Path) -> Path:
        from ..report.html import generate_html_report

        destination = Path(path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        _write_atomic(
            destination,
            lambda temporary: generate_html_report(self, temporary),
        )
        self.report_path = destination
        return destination

    def export(self, output_dir: str | Path | None = None) -> BacktestResult:
        directory = (
            Path(output_dir) if output_dir is not None else self.config.output_dir
        )
        if directory is None:
            raise ValueError("an output directory is required")
        directory.mkdir(parents=True, exist_ok=True)
        self.export_positions(directory / "daily_positions.csv")
        if self.config.generate_report:
            self.export_report(directory / "report.html")
        return self

    @property
    def final_value(self) -> float:
        if self.performance.empty:
            return self.config.capital_base
        return float(self.performance["portfolio_value"].iloc[-1])

    def summary(self) -> pd.Series:
        return pd.Series(self.metrics, name="value")
