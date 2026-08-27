"""Structured backtest outputs and file export helpers."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

from ..foundation.config import BacktestConfig


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
        self.daily_positions.to_csv(
            destination,
            index=False,
            encoding="utf-8-sig",
            float_format="%.8f",
        )
        self.positions_path = destination
        return destination

    def export_report(self, path: str | Path) -> Path:
        from ..report.html import generate_html_report

        destination = Path(path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        generate_html_report(self, destination)
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
