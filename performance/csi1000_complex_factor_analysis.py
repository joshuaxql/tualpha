"""Profile a complex CSI 1000 factor analysis and generate its report."""

from __future__ import annotations

import argparse
import csv
import json
import platform
from collections import defaultdict
from pathlib import Path
from time import perf_counter
from typing import Any
from unittest.mock import patch

import numpy as np
import pandas as pd

import tualpha
from tualpha import run_factor_analysis
from tualpha.analysis import factor as factor_module
from tualpha.analysis.factor import FactorAnalysisResult
from tualpha.data.research import FactorData

START_DATE = "2017-01-01"
END_DATE = "2026-01-01"
INDEX_CODE = "000852.SH"
FACTOR_NAME = "价量波动市值复合因子"
FACTOR_EXPRESSION = (
    "RANK("
    "0.30*TS_ZSCORE(RETURNS($close,20),60)"
    "-0.25*TS_ZSCORE(STD(RETURNS($close,1),20),60)"
    "+0.20*TS_ZSCORE($volume/MA($volume,20),20)"
    "-0.15*TS_ZSCORE(ATR($close,14)/$close,60)"
    "+0.10*RANK(1/$daily_basic.total_mv)"
    ")"
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="生成中证1000复杂因子报告并记录各阶段耗时",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--start", default=START_DATE)
    parser.add_argument("--end", default=END_DATE)
    parser.add_argument("--bundle-root", default="~/.tualpha")
    parser.add_argument("--bundle-name", default="tualpha")
    parser.add_argument("--label", default="run")
    parser.add_argument(
        "--output-root",
        default="outputs/performance/csi1000_complex_factor",
    )
    parser.add_argument(
        "--report-dir",
        default="outputs/factor_reports/csi1000_complex_factor",
    )
    parser.add_argument("--column-cache-mib", type=int, default=1024)
    parser.add_argument("--plotly-js", choices=("inline", "cdn"), default="inline")
    return parser


def _finite_checksum(values: Any) -> float:
    array = np.asarray(values, dtype=float)
    return float(np.nansum(array))


def main() -> None:
    args = _parser().parse_args()
    timings: dict[str, float] = defaultdict(float)
    history_calls: list[dict[str, Any]] = []

    original_init = FactorData.__init__
    original_history_arrays = FactorData.history_arrays
    original_analyze = factor_module.analyze_factor_data
    original_metrics = FactorAnalysisResult.export_metrics
    original_report = FactorAnalysisResult.export_report

    def timed_init(self: FactorData, *values: Any, **kwargs: Any) -> None:
        started = perf_counter()
        original_init(self, *values, **kwargs)
        timings["data_initialization"] += perf_counter() - started

    def timed_history_arrays(
        self: FactorData,
        expressions: Any,
        *,
        allow_future: bool = False,
    ) -> Any:
        started = perf_counter()
        result = original_history_arrays(
            self,
            expressions,
            allow_future=allow_future,
        )
        elapsed = perf_counter() - started
        history_calls.append(
            {
                "interface": "history_arrays",
                "allow_future": allow_future,
                "seconds": elapsed,
            }
        )
        timings["data_and_expression_evaluation"] += elapsed
        return result

    def timed_analyze(*values: Any, **kwargs: Any) -> FactorAnalysisResult:
        started = perf_counter()
        result = original_analyze(*values, **kwargs)
        timings["analysis_including_export"] += perf_counter() - started
        return result

    def timed_metrics(
        self: FactorAnalysisResult,
        path: str | Path,
    ) -> Path:
        started = perf_counter()
        result = original_metrics(self, path)
        timings["csv_export"] += perf_counter() - started
        return result

    def timed_report(
        self: FactorAnalysisResult,
        path: str | Path,
    ) -> Path:
        started = perf_counter()
        result = original_report(self, path)
        timings["html_report_export"] += perf_counter() - started
        return result

    total_started = perf_counter()
    with (
        patch.object(FactorData, "__init__", timed_init),
        patch.object(FactorData, "history_arrays", timed_history_arrays),
        patch.object(factor_module, "analyze_factor_data", timed_analyze),
        patch.object(FactorAnalysisResult, "export_metrics", timed_metrics),
        patch.object(FactorAnalysisResult, "export_report", timed_report),
    ):
        result = run_factor_analysis(
            {FACTOR_NAME: FACTOR_EXPRESSION},
            start=args.start,
            end=args.end,
            index_code=INDEX_CODE,
            periods=[1, 5, 10],
            quantiles=5,
            minimum_observations=5,
            adjustment="qfq",
            exclude_st=True,
            min_listed_days=365,
            exclude_suspended=True,
            industry_neutral=True,
            market_cap_neutral=True,
            column_cache_mib=args.column_cache_mib,
            output_dir=args.report_dir,
            title="中证1000 · 价量波动市值复合因子",
            plotly_js=args.plotly_js,
        )
    total = perf_counter() - total_started
    analysis_total = timings.get("analysis_including_export", 0.0)
    timings["factor_statistics"] = max(
        0.0,
        analysis_total
        - timings.get("csv_export", 0.0)
        - timings.get("html_report_export", 0.0),
    )
    accounted = (
        timings.get("data_initialization", 0.0)
        + timings.get("data_and_expression_evaluation", 0.0)
        + analysis_total
    )
    timings["matrix_preparation_and_cleanup"] = max(0.0, total - accounted)
    timings["total"] = total

    payload = {
        "benchmark": "中证1000复杂因子分析",
        "label": args.label,
        "tualpha_version": tualpha.__version__,
        "python_version": platform.python_version(),
        "start": args.start,
        "end": args.end,
        "index_code": INDEX_CODE,
        "factor_name": FACTOR_NAME,
        "factor_expression": FACTOR_EXPRESSION,
        "periods": [1, 5, 10],
        "adjustment": "qfq",
        "filters": {
            "exclude_st": True,
            "min_listed_days": 365,
            "exclude_suspended": True,
        },
        "neutralization": ["industry", "market_cap"],
        "column_cache_mib": args.column_cache_mib,
        "history_calls": [
            {**item, "seconds": round(float(item["seconds"]), 6)}
            for item in history_calls
        ],
        "timings_seconds": {name: round(value, 6) for name, value in timings.items()},
        "summary_checksum": _finite_checksum(
            result.summary_metrics.select_dtypes(include="number")
        ),
        "daily_metrics_checksum": _finite_checksum(
            result.daily_metrics.select_dtypes(include="number")
        ),
        "daily_metric_rows": len(result.daily_metrics),
        "report_path": str(result.report_path),
        "metrics_path": str(result.metrics_path),
    }
    destination = Path(args.output_root) / f"benchmark_{args.label}.json"
    destination.parent.mkdir(parents=True, exist_ok=True)
    baseline_path = Path(args.output_root) / "benchmark_before.json"
    if args.label == "after" and baseline_path.is_file():
        baseline = json.loads(baseline_path.read_text(encoding="utf-8"))
        before_metrics = pd.read_csv(baseline["metrics_path"])
        after_metrics = pd.read_csv(result.metrics_path)
        numeric_columns = before_metrics.select_dtypes(include="number").columns
        maximum_differences = {}
        for column in numeric_columns:
            difference = np.abs(
                before_metrics[column].to_numpy(dtype=float)
                - after_metrics[column].to_numpy(dtype=float)
            )
            maximum_differences[column] = (
                float(np.nanmax(difference))
                if bool(np.isfinite(difference).any())
                else 0.0
            )
        payload["baseline_equivalence"] = {
            "same_shape": before_metrics.shape == after_metrics.shape,
            "same_non_numeric_values": before_metrics.select_dtypes(
                exclude="number"
            ).equals(after_metrics.select_dtypes(exclude="number")),
            "same_nan_masks": all(
                before_metrics[column].isna().equals(after_metrics[column].isna())
                for column in numeric_columns
            ),
            "maximum_absolute_difference_by_column": maximum_differences,
        }
        stages = (
            "data_initialization",
            "data_and_expression_evaluation",
            "factor_statistics",
            "csv_export",
            "html_report_export",
            "matrix_preparation_and_cleanup",
            "total",
        )
        comparison_path = Path(args.output_root) / "stage_comparison.csv"
        with comparison_path.open("w", newline="", encoding="utf-8-sig") as stream:
            writer = csv.DictWriter(
                stream,
                fieldnames=(
                    "stage",
                    "before_seconds",
                    "after_seconds",
                    "saved_seconds",
                    "improvement_percent",
                ),
            )
            writer.writeheader()
            for stage in stages:
                before = float(baseline["timings_seconds"][stage])
                after = float(payload["timings_seconds"][stage])
                writer.writerow(
                    {
                        "stage": stage,
                        "before_seconds": round(before, 6),
                        "after_seconds": round(after, 6),
                        "saved_seconds": round(before - after, 6),
                        "improvement_percent": round(
                            (before - after) / before * 100.0 if before else 0.0,
                            3,
                        ),
                    }
                )
        print(f"阶段对比：{comparison_path}")
    destination.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    print(f"\n性能记录：{destination}")


if __name__ == "__main__":
    main()
