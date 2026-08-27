from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import pytest

from tualpha.config import BacktestConfig
from tualpha.reporting import (
    _attribution_rows,
    _geometric_attribution,
    _geometric_link,
)
from tualpha.result import BacktestResult


def _export_result() -> BacktestResult:
    dates = pd.date_range("2024-01-02", periods=2, freq="B")
    return BacktestResult(
        config=BacktestConfig(start=dates[0], end=dates[-1], generate_report=True),
        performance=pd.DataFrame(
            {
                "returns": [0.0, 0.01],
                "portfolio_value": [100_000.0, 101_000.0],
                "algorithm_period_return": [0.0, 0.01],
                "commission": [0.0, 0.0],
                "stamp_tax": [0.0, 0.0],
                "transfer_fee": [0.0, 0.0],
                "fees": [0.0, 0.0],
                "turnover": [0.0, 0.0],
            },
            index=dates,
        ),
        daily_positions=pd.DataFrame(
            [{"date": dates[0], "record_type": "CASH", "ts_code": "CASH"}]
        ),
        orders=pd.DataFrame(),
        transactions=pd.DataFrame(),
        closed_trades=pd.DataFrame(),
        metrics={"total_return": 0.01},
    )


def test_position_export_is_atomic_when_csv_write_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    result = _export_result()
    destination = tmp_path / "daily_positions.csv"
    destination.write_text("previous", encoding="utf-8")

    def fail_write(self: pd.DataFrame, path: Path, **kwargs: object) -> None:
        del self, kwargs
        Path(path).write_text("partial", encoding="utf-8")
        raise OSError("simulated disk failure")

    monkeypatch.setattr(pd.DataFrame, "to_csv", fail_write)
    with pytest.raises(OSError, match="simulated disk failure"):
        result.export_positions(destination)

    assert destination.read_text(encoding="utf-8") == "previous"
    assert not list(tmp_path.glob(".*.tmp"))
    assert result.positions_path is None


def test_report_export_is_atomic_when_generation_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    result = _export_result()
    destination = tmp_path / "report.html"
    destination.write_text("previous", encoding="utf-8")

    def fail_report(result: BacktestResult, path: Path) -> Path:
        del result
        Path(path).write_text("partial", encoding="utf-8")
        raise OSError("simulated report failure")

    monkeypatch.setattr("tualpha.report.html.generate_html_report", fail_report)
    with pytest.raises(OSError, match="simulated report failure"):
        result.export_report(destination)

    assert destination.read_text(encoding="utf-8") == "previous"
    assert not list(tmp_path.glob(".*.tmp"))
    assert result.report_path is None


def test_output_publication_retries_transient_windows_file_lock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    result = _export_result()
    destination = tmp_path / "daily_positions.csv"
    original_replace = os.replace
    attempts = 0

    def flaky_replace(source: Path, target: Path) -> None:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise PermissionError("simulated Windows file lock")
        original_replace(source, target)

    monkeypatch.setattr("tualpha.analysis.result.os.replace", flaky_replace)
    monkeypatch.setattr("tualpha.analysis.result.time.sleep", lambda delay: None)

    result.export_positions(destination)

    assert attempts == 3
    assert destination.read_bytes().startswith(b"\xef\xbb\xbf")


def test_geometric_link_compounds_positive_and_negative_periods() -> None:
    returns = pd.Series([0.05, -0.03])
    assert _geometric_link(returns) == pytest.approx(1.05 * 0.97 - 1.0)


def test_geometric_attribution_sums_daily_weighted_asset_returns() -> None:
    dates = pd.date_range("2024-01-02", periods=2, freq="B")
    performance = pd.DataFrame(
        {"returns": [0.0, 0.0], "portfolio_value": [100_000.0, 100_000.0]},
        index=dates,
    )
    positions = pd.DataFrame(
        [
            {
                "date": date,
                "record_type": "CASH",
                "ts_code": "CASH",
                "quantity": 0.0,
                "market_value": cash,
                "weight": cash_weight,
                "asset_return": 0.0,
            }
            for date, cash, cash_weight in zip(
                dates, [20_000.0, 0.0], [0.2, 0.0], strict=True
            )
        ]
        + [
            {
                "date": dates[0],
                "record_type": "POSITION",
                "ts_code": "A",
                "quantity": 100,
                "market_value": 30_000,
                "weight": 0.3,
                "asset_return": 0.01,
            },
            {
                "date": dates[0],
                "record_type": "POSITION",
                "ts_code": "B",
                "quantity": 100,
                "market_value": 50_000,
                "weight": 0.5,
                "asset_return": -0.02,
            },
            {
                "date": dates[1],
                "record_type": "POSITION",
                "ts_code": "A",
                "quantity": 100,
                "market_value": 10_000,
                "weight": 0.1,
                "asset_return": -0.01,
            },
            {
                "date": dates[1],
                "record_type": "POSITION",
                "ts_code": "B",
                "quantity": 100,
                "market_value": 90_000,
                "weight": 0.9,
                "asset_return": 0.02,
            },
        ]
    )
    result = BacktestResult(
        config=BacktestConfig(start=dates[0], end=dates[-1], generate_report=False),
        performance=performance,
        daily_positions=positions,
        orders=pd.DataFrame(),
        transactions=pd.DataFrame(),
        closed_trades=pd.DataFrame(),
        metrics={},
    )

    attribution = _geometric_attribution(result)
    assert attribution["A"] == pytest.approx(0.01 * 0.3 + (-0.01) * 0.1)
    assert attribution["B"] == pytest.approx((-0.02) * 0.5 + 0.02 * 0.9)
    assert attribution["CASH"] == 0.0


def test_cash_holding_days_count_only_fully_cash_sessions() -> None:
    dates = pd.date_range("2024-01-02", periods=3, freq="B")
    positions = pd.DataFrame(
        [
            {
                "date": date,
                "record_type": "CASH",
                "ts_code": "CASH",
                "quantity": 0.0,
                "market_value": cash,
                "weight": cash / 100_000.0,
            }
            for date, cash in zip(dates, [100_000.0, 20_000.0, 100_000.0], strict=True)
        ]
        + [
            {
                "date": dates[1],
                "record_type": "POSITION",
                "ts_code": "LIVE",
                "quantity": 100.0,
                "market_value": 80_000.0,
                "weight": 0.8,
            },
            {
                "date": dates[2],
                "record_type": "POSITION",
                "ts_code": "DELISTED_AUDIT",
                "quantity": 100.0,
                "market_value": 0.0,
                "weight": 0.0,
            },
        ]
    )
    result = BacktestResult(
        config=BacktestConfig(start=dates[0], end=dates[-1], generate_report=False),
        performance=pd.DataFrame(
            {"returns": [0.0, 0.0, 0.0], "portfolio_value": [100_000.0] * 3},
            index=dates,
        ),
        daily_positions=positions,
        orders=pd.DataFrame(),
        transactions=pd.DataFrame(),
        closed_trades=pd.DataFrame(),
        metrics={},
    )

    rows = _attribution_rows(result)

    assert "<tr><td>CASH</td><td>0</td><td>2</td>" in rows
