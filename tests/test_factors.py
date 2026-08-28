from __future__ import annotations

from pathlib import Path
from runpy import run_path

import numpy as np
import pandas as pd
import pytest

from tualpha import (
    analyze_factor_data,
    factor_data,
    neutralize_factor_values,
    run_factor_analysis,
)
from tualpha.assets import AssetFinder
from tualpha.calendar import ChinaTradingCalendar
from tualpha.data import BarData, TushareDataPortal
from tualpha.data.factors import (
    available_operators,
    compile_expression,
    evaluate_expressions,
)
from tualpha.report.factor_html import (
    _factor_weighted_cumulative,
    _ic_diagnostics,
    _mean_return_by_quantile,
    _neutralization_label,
    _quantile_cumulative,
    _sector_ic_chart,
)

OPERATOR_EXPRESSIONS = run_path(
    Path(__file__).parents[1] / "performance" / "csi1000_factor_benchmark.py"
)["OPERATOR_EXPRESSIONS"]


def _factor_inputs(rows: int = 140, columns: int = 6) -> dict[str, pd.DataFrame]:
    random = np.random.default_rng(7)
    index = pd.bdate_range("2023-01-02", periods=rows)
    names = [f"asset_{position}" for position in range(columns)]
    close = 20 + np.cumsum(random.normal(0.03, 0.4, (rows, columns)), axis=0)
    close = np.maximum(close, 1.0)
    open_ = close * (1 + random.normal(0, 0.01, close.shape))
    high = np.maximum(open_, close) * (1 + random.uniform(0, 0.02, close.shape))
    low = np.minimum(open_, close) * (1 - random.uniform(0, 0.02, close.shape))
    volume = random.uniform(1_000, 100_000, close.shape)
    return {
        "close": pd.DataFrame(close, index=index, columns=names),
        "open": pd.DataFrame(open_, index=index, columns=names),
        "high": pd.DataFrame(high, index=index, columns=names),
        "low": pd.DataFrame(low, index=index, columns=names),
        "volume": pd.DataFrame(volume, index=index, columns=names),
    }


def test_factor_compiler_tracks_dependencies_and_rejects_python() -> None:
    expression = compile_expression("RANK(MA($close,20)/$daily_basic.total_mv)")

    assert expression.fields == ("close", "daily_basic.total_mv")
    assert expression.lookback == 19
    assert expression.lookahead == 0
    assert compile_expression("FUTURE_RETURNS($close,5)").lookahead == 5
    with pytest.raises(TypeError, match="factor functions must be called by name"):
        compile_expression("__import__('os').system('echo unsafe')")


def test_every_documented_operator_evaluates_vectorized() -> None:
    inputs = _factor_inputs()

    assert set(OPERATOR_EXPRESSIONS) == set(available_operators())
    for name, expression in OPERATOR_EXPRESSIONS.items():
        result = evaluate_expressions(expression, inputs)[expression]
        assert result.shape == inputs["close"].shape, name
        assert result.index.equals(inputs["close"].index), name
        assert result.columns.equals(inputs["close"].columns), name


def test_chunked_rolling_operators_match_reference_formulas() -> None:
    close = _factor_inputs(rows=30, columns=3)["close"]
    inputs = {"close": close}
    expressions = [
        "AVEDEV($close,5)",
        "TS_RANK($close,5)",
        "TS_ARGMAX($close,5)",
        "WMA($close,5)",
        "SLOPE($close,5)",
    ]
    actual = evaluate_expressions(expressions, inputs)
    weights = np.arange(1, 6, dtype=float)
    weights /= weights.sum()
    centered = np.arange(5, dtype=float) - 2.0
    references = {
        expressions[0]: close.rolling(5, min_periods=5).apply(
            lambda values: np.mean(np.abs(values - np.mean(values))), raw=True
        ),
        expressions[1]: close.rolling(5, min_periods=5).apply(
            lambda values: pd.Series(values).rank(pct=True).iloc[-1], raw=True
        ),
        expressions[2]: close.rolling(5, min_periods=5).apply(np.argmax, raw=True),
        expressions[3]: close.rolling(5, min_periods=5).apply(
            lambda values: np.dot(values, weights), raw=True
        ),
        expressions[4]: close.rolling(5, min_periods=5).apply(
            lambda values: np.dot(values, centered) / np.dot(centered, centered),
            raw=True,
        ),
    }
    for expression, expected in references.items():
        pd.testing.assert_frame_equal(actual[expression], expected)


def test_callback_history_accepts_factor_expressions(data_root: Path) -> None:
    finder = AssetFinder(data_root)
    calendar = ChinaTradingCalendar(data_root)
    assets = [
        finder.retrieve_asset("000001.SZ"),
        finder.retrieve_asset("688001.SH"),
    ]
    portal = TushareDataPortal(data_root, finder, calendar, "raw", "2024-01-08")
    data = BarData(portal)
    data._set_session("2024-01-04")

    expressions = ["RANK($close/$open)", "1/$daily_basic.pe"]
    result = data.history(assets, expressions, 3)

    assert result.shape == (3, 4)
    assert list(result.columns.names) == ["asset", "field"]
    assert result[("000001.SZ", "1/$daily_basic.pe")].tolist() == pytest.approx(
        [0.1, 0.1, 0.1]
    )
    single = data.history(assets[0], "RETURNS($close,1)", 2)
    assert single.name == "000001.SZ"
    assert single.iloc[-1] == pytest.approx(0.0)
    assert "RANK" in data.available_operators()
    portal.close()


def test_factor_data_history_uses_pit_index_and_daily_filters(
    data_root: Path,
) -> None:
    with factor_data(
        start="2024-01-03",
        end="2024-01-05",
        index_code="000300.SH",
        bundle_root=data_root,
        adjustment="raw",
        exclude_st=True,
        exclude_suspended=True,
    ) as data:
        data.prefetch(["RANK($close/$open)", "1/$daily_basic.pe"])
        result = data.history(["RANK($close/$open)", "1/$daily_basic.pe"])
        arrays = data.history_arrays("RANK($close/$open)")
        mask = data.universe_mask()

    assert result.shape == (3, 4)
    pd.testing.assert_frame_equal(
        arrays["RANK($close/$open)"],
        result.xs("RANK($close/$open)", axis=1, level="field"),
    )
    assert bool(mask.loc["2024-01-03", "000001.SZ"])
    assert not bool(mask.loc["2024-01-04", "000001.SZ"])
    assert not bool(mask.loc["2024-01-05", "000001.SZ"])
    assert bool(mask.loc["2024-01-05", "688001.SH"])
    assert pd.isna(result.loc["2024-01-04", ("000001.SZ", "1/$daily_basic.pe")])


def test_combined_future_history_preserves_qfq_signal_reference(
    data_root: Path,
) -> None:
    with factor_data(
        start="2024-01-02",
        end="2024-01-05",
        assets=["000001.SZ", "688001.SH"],
        bundle_root=data_root,
        adjustment="qfq",
    ) as data:
        signal = data.history("$close")
        forward = data.history("FUTURE_RETURNS($close,1)", allow_future=True)
        combined = data.history_arrays(
            ["$close", "FUTURE_RETURNS($close,1)"],
            allow_future=True,
        )

    pd.testing.assert_frame_equal(combined["$close"], signal, check_names=False)
    pd.testing.assert_frame_equal(
        combined["FUTURE_RETURNS($close,1)"],
        forward,
        check_names=False,
    )


def test_factor_analysis_calculates_ic_rankic_and_exports(tmp_path: Path) -> None:
    random = np.random.default_rng(11)
    dates = pd.bdate_range("2024-01-02", periods=30)
    assets = [f"A{value}" for value in range(8)]
    values = pd.DataFrame(
        random.normal(size=(len(dates), len(assets))),
        index=dates,
        columns=assets,
    )
    factors = pd.concat({"quality": values}, axis=1).swaplevel(0, 1, axis=1)
    factors.columns.names = ["asset", "field"]
    forward = {
        period: values * 0.02 + random.normal(0, 0.0001, values.shape)
        for period in (1, 5, 10)
    }

    result = analyze_factor_data(
        factors,
        forward,
        factor_expressions={"quality": "RANK($close)"},
        quantiles=4,
        minimum_observations=5,
        metadata={
            "index_code": "000852.SH",
            "index_name": "中证1000",
            "exclude_st": True,
            "exclude_suspended": True,
            "min_listed_days": 365,
        },
        output_dir=tmp_path,
    )

    assert result.summary_metrics.iloc[0]["mean_ic"] > 0.99
    assert result.summary_metrics.iloc[0]["mean_rank_ic"] > 0.99
    assert set(result.daily_metrics.columns) >= {
        "date",
        "factor",
        "period",
        "ic",
        "rank_ic",
        "valid_count",
        "long_short_return",
        "top_quantile_turnover",
    }
    assert (tmp_path / "daily_factor_metrics.csv").is_file()
    assert (tmp_path / "factor_report.html").is_file()
    report = (tmp_path / "factor_report.html").read_text("utf-8")
    assert "因子分析" in report
    assert "RANK($close)" in report
    assert "Returns Analysis" in report
    assert "Information Analysis" in report
    assert "Turnover Analysis" in report
    assert "行业分析" in report
    assert 'class="container"' in report
    assert "--primary:#2c3e50" in report
    assert "TuAlpha Report · Powered by Plotly" in report
    assert "Factor Evidence Book" not in report
    weighted_figure = _factor_weighted_cumulative(result)
    quantile_figure = _quantile_cumulative(result)
    weighted_titles = {
        annotation.text for annotation in weighted_figure.layout.annotations
    }
    quantile_titles = {
        annotation.text for annotation in quantile_figure.layout.annotations
    }
    for period in (1, 5, 10):
        assert f"因子加权多空组合累计收益（{period}日）" in weighted_titles
        assert f"分位组合累计收益（{period}日）" in quantile_titles
    assert len(weighted_figure.data) == 3
    assert len(quantile_figure.data) == 12
    diagnostics = _ic_diagnostics(result)
    assert diagnostics.layout.height == 1110
    assert diagnostics.layout.xaxis.title.text is None
    assert diagnostics.layout.xaxis2.title.text is None
    assert diagnostics.layout.xaxis5.title.text == "IC"
    assert diagnostics.layout.xaxis6.title.text == "正态理论分位"
    assert sum(trace.showlegend is True for trace in diagnostics.data) == 2
    assert "section-note" not in report
    assert "计算口径" not in report
    assert 'id="ic-diagnostics"' in report
    assert 'id="sector-ic"' in report
    assert '<span class="summary-line">中证1000</span>' in report
    assert '<span class="summary-line">标的：000852.SH</span>' in report
    assert "1000只" not in report
    assert '<span class="summary-line">ST：剔除</span>' in report
    assert '<span class="summary-line">停牌：剔除</span>' in report
    assert '<span class="summary-line">上市：≥365天</span>' in report
    assert _neutralization_label(result) == "无"
    result.metadata["industry_neutral"] = True
    result.metadata["market_cap_neutral"] = True
    assert _neutralization_label(result) == "行业 + 市值"


def test_factor_report_requires_exactly_one_factor(tmp_path: Path) -> None:
    dates = pd.bdate_range("2024-01-02", periods=5)
    values = pd.DataFrame(
        np.arange(20, dtype=float).reshape(5, 4),
        index=dates,
        columns=["A", "B", "C", "D"],
    )
    factors = pd.concat({"quality": values, "value": -values}, axis=1).swaplevel(
        0, 1, axis=1
    )
    factors.columns.names = ["asset", "field"]

    with pytest.raises(ValueError, match="exactly one factor"):
        analyze_factor_data(
            factors,
            {1: values},
            minimum_observations=2,
            output_dir=tmp_path / "mixed",
        )

    assert not (tmp_path / "mixed").exists()


def test_quantile_chart_dailyizes_without_compounding_forward_returns() -> None:
    dates = pd.bdate_range("2024-01-02", periods=3)
    assets = ["A", "B", "C", "D"]
    factor_values = pd.DataFrame(
        [[1.0, 2.0, 3.0, 4.0]] * len(dates),
        index=dates,
        columns=assets,
    )
    factors = pd.concat({"quality": factor_values}, axis=1).swaplevel(0, 1, axis=1)
    factors.columns.names = ["asset", "field"]
    forward = {
        5: pd.DataFrame(
            [[0.01, 0.01, 0.02, 0.02]] * len(dates),
            index=dates,
            columns=assets,
        )
    }

    result = analyze_factor_data(
        factors,
        forward,
        quantiles=2,
        minimum_observations=2,
    )
    figure = _mean_return_by_quantile(result)

    assert figure.layout.title.text == "各分位平均日收益"
    assert tuple(figure.data[0].x) == ("Q1", "Q2")
    expected = np.expm1(np.log1p([0.01, 0.02]) / 5) * 10_000
    np.testing.assert_allclose(figure.data[0].y, expected)


def test_neutralization_is_orthogonal_to_industry_and_log_size() -> None:
    dates = pd.bdate_range("2024-01-02", periods=2)
    assets = ["A", "B", "C", "D", "E", "F"]
    industries = pd.DataFrame(
        [["I1", "I1", "I1", "I2", "I2", "I2"]] * 2,
        index=dates,
        columns=assets,
    )
    market_caps = pd.DataFrame(
        [[100.0, 200.0, 400.0, 100.0, 200.0, 400.0]] * 2,
        index=dates,
        columns=assets,
    )
    log_size = np.log(market_caps)
    factors = pd.DataFrame(
        [
            [1.0, 2.0, 5.0, 11.0, 13.0, 18.0],
            [3.0, 4.0, 8.0, 20.0, 21.0, 29.0],
        ],
        index=dates,
        columns=assets,
    )

    residual = neutralize_factor_values(
        factors,
        industries=industries,
        market_caps=market_caps,
    )

    for date in dates:
        for industry in ("I1", "I2"):
            members = industries.loc[date].eq(industry)
            assert residual.loc[date, members].mean() == pytest.approx(0.0, abs=1e-12)
        assert float((residual.loc[date] * log_size.loc[date]).sum()) == pytest.approx(
            0.0, abs=1e-12
        )


def test_factor_analysis_reports_sector_statistics_and_neutralization() -> None:
    dates = pd.bdate_range("2024-01-02", periods=4)
    assets = ["A", "B", "C", "D", "E", "F"]
    values = pd.DataFrame(
        [[1.0, 2.0, 3.0, 4.0, 5.0, 6.0]] * len(dates),
        index=dates,
        columns=assets,
    )
    factors = pd.concat({"quality": values}, axis=1).swaplevel(0, 1, axis=1)
    factors.columns.names = ["asset", "field"]
    industries = pd.DataFrame(
        [["I1", "I1", "I1", "I2", "I2", "I2"]] * len(dates),
        index=dates,
        columns=assets,
    )

    result = analyze_factor_data(
        factors,
        {1: values * 0.01},
        industries=industries,
        industry_neutral=True,
        quantiles=3,
        minimum_observations=2,
    )

    assert result.metadata["industry_neutral"] is True
    assert set(result.sector_ic["sector"]) == {"I1", "I2"}
    assert set(result.sector_quantile_returns["sector"]) == {"I1", "I2"}
    assert result.sector_ic.set_index("sector")["mean_ic"].to_dict() == pytest.approx(
        {"I1": 1.0, "I2": 1.0}
    )
    sector_quantiles = result.sector_quantile_returns.set_index(["sector", "quantile"])[
        "mean_return"
    ]
    assert sector_quantiles.to_dict() == pytest.approx(
        {
            ("I1", 1): 0.01,
            ("I1", 2): 0.02,
            ("I1", 3): 0.03,
            ("I2", 1): 0.04,
            ("I2", 2): 0.05,
            ("I2", 3): 0.06,
        }
    )
    assert {"bottom_quantile_turnover", "factor_rank_autocorrelation"} <= set(
        result.daily_metrics
    )
    sector_figure = _sector_ic_chart(result)
    assert all(trace.orientation == "h" for trace in sector_figure.data)
    assert sector_figure.layout.yaxis.automargin is True
    assert sector_figure.layout.height >= 600


def test_run_factor_analysis_reads_future_labels_offline(
    data_root: Path,
    tmp_path: Path,
) -> None:
    result = run_factor_analysis(
        {"intraday": "RANK($close/$open)"},
        start="2024-01-02",
        end="2024-01-05",
        assets=["000001.SZ", "688001.SH"],
        periods=[1, 2],
        quantiles=2,
        minimum_observations=2,
        bundle_root=data_root,
        exclude_st=False,
        min_listed_days=0,
        exclude_suspended=False,
        output_dir=tmp_path,
    )

    assert result.factors == ("intraday",)
    assert result.periods == (1, 2)
    assert len(result.daily_metrics) == 8
    assert result.metadata["adjustment"] == "raw"
    assert result.metrics_path == tmp_path / "daily_factor_metrics.csv"
    assert result.report_path == tmp_path / "factor_report.html"
