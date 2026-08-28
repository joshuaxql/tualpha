"""Standalone editorial Plotly report for one factor."""

from __future__ import annotations

from datetime import datetime
from html import escape
from pathlib import Path
from statistics import NormalDist
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from .charts import base_layout, empty_figure, figure_html
from .formatting import COLORS, metric_card, number, percent

if TYPE_CHECKING:
    from ..analysis.factor import FactorAnalysisResult

PERIOD_COLORS = (
    COLORS["accent"],
    "#8e44ad",
    COLORS["success"],
    COLORS["danger"],
    COLORS["muted"],
)
QUANTILE_COLORS = (
    COLORS["danger"],
    "#e67e22",
    "#f1c40f",
    COLORS["success"],
    COLORS["accent"],
)
ROLLING_COLOR = "#8e44ad"


def _finish(figure: go.Figure, height: int = 420) -> go.Figure:
    figure = base_layout(figure, height=height)
    figure.update_layout(
        colorway=list(PERIOD_COLORS),
    )
    return figure


def _dailyize(values: pd.Series | np.ndarray, period: int) -> np.ndarray:
    source = np.asarray(values, dtype=float)
    output = np.full(source.shape, np.nan, dtype=float)
    valid = np.isfinite(source) & (source > -1.0)
    output[valid] = np.expm1(np.log1p(source[valid]) / int(period))
    return output


def _mean_return_by_quantile(result: FactorAnalysisResult) -> go.Figure:
    if result.quantile_returns.empty:
        return empty_figure("分位平均日收益", "没有可用的分位收益数据")
    means = (
        result.quantile_returns.groupby(["period", "quantile"], sort=False)["return"]
        .mean()
        .rename("mean_return")
        .reset_index()
    )
    figure = go.Figure()
    for index, (period, group) in enumerate(means.groupby("period", sort=False)):
        figure.add_trace(
            go.Bar(
                x=[f"Q{int(value)}" for value in group["quantile"]],
                y=_dailyize(group["mean_return"], int(period)) * 10_000.0,
                name=f"{int(period)}日",
                marker_color=PERIOD_COLORS[index % len(PERIOD_COLORS)],
                hovertemplate="%{x}<br>日均收益 %{y:.3f} bps<extra></extra>",
            )
        )
    figure.add_hline(y=0.0, line={"color": COLORS["muted"], "dash": "dot"})
    figure.update_layout(title="各分位平均日收益", barmode="group")
    figure.update_yaxes(title="bps")
    return _finish(figure)


def _quantile_distribution(result: FactorAnalysisResult) -> go.Figure:
    if result.quantile_returns.empty:
        return empty_figure("分位日收益分布", "没有可用的分位收益数据")
    figure = go.Figure()
    for index, (period, group) in enumerate(
        result.quantile_returns.groupby("period", sort=False)
    ):
        figure.add_trace(
            go.Violin(
                x=[f"Q{int(value)}" for value in group["quantile"]],
                y=_dailyize(group["return"], int(period)) * 10_000.0,
                name=f"{int(period)}日",
                legendgroup=str(period),
                scalegroup=str(period),
                box_visible=True,
                meanline_visible=True,
                points=False,
                opacity=0.68,
                line_color=PERIOD_COLORS[index % len(PERIOD_COLORS)],
            )
        )
    figure.update_layout(title="每日分位收益分布", violinmode="group")
    figure.update_yaxes(title="日均收益（bps）")
    return _finish(figure, height=500)


def _factor_weighted_cumulative(result: FactorAnalysisResult) -> go.Figure:
    periods = list(result.periods)
    figure = make_subplots(
        rows=len(periods),
        cols=1,
        shared_xaxes=True,
        vertical_spacing=min(0.12, 0.2 / max(1, len(periods))),
        subplot_titles=[
            f"因子加权多空组合累计收益（{period}日）" for period in periods
        ],
    )
    for row, period in enumerate(periods, start=1):
        group = result.daily_metrics[
            result.daily_metrics["period"] == period
        ].sort_values("date")
        daily_returns = pd.Series(
            _dailyize(group["factor_weighted_return"], period),
            index=group.index,
        )
        cumulative = (1.0 + daily_returns.fillna(0.0)).cumprod()
        figure.add_trace(
            go.Scatter(
                x=group["date"],
                y=cumulative,
                name=f"{period}日",
                line={
                    "color": PERIOD_COLORS[(row - 1) % len(PERIOD_COLORS)],
                    "width": 2.2,
                },
            ),
            row=row,
            col=1,
        )
        figure.update_yaxes(title="净值", tickformat=".2f", row=row, col=1)
    figure.update_layout(title="因子加权多空组合累计收益")
    return _finish(figure, height=max(500, 260 * len(periods)))


def _quantile_cumulative(result: FactorAnalysisResult) -> go.Figure:
    periods = list(result.periods)
    figure = make_subplots(
        rows=len(periods),
        cols=1,
        shared_xaxes=True,
        vertical_spacing=min(0.12, 0.2 / max(1, len(periods))),
        subplot_titles=[f"分位组合累计收益（{period}日）" for period in periods],
    )
    for row, period in enumerate(periods, start=1):
        group = result.quantile_returns[result.quantile_returns["period"] == period]
        for quantile, values in group.groupby("quantile", sort=True):
            ordered = values.sort_values("date")
            daily_returns = pd.Series(
                _dailyize(ordered["return"], period),
                index=ordered.index,
            )
            cumulative = (1.0 + daily_returns.fillna(0.0)).cumprod()
            figure.add_trace(
                go.Scatter(
                    x=ordered["date"],
                    y=cumulative,
                    name=f"Q{int(quantile)}",
                    legendgroup=f"Q{int(quantile)}",
                    showlegend=row == 1,
                    line={
                        "color": QUANTILE_COLORS[
                            (int(quantile) - 1) % len(QUANTILE_COLORS)
                        ],
                        "width": 1.8,
                    },
                ),
                row=row,
                col=1,
            )
        figure.update_yaxes(
            title="净值",
            type="log",
            tickformat=".2f",
            row=row,
            col=1,
        )
    figure.update_layout(title="分位组合累计收益")
    return _finish(figure, height=max(500, 260 * len(periods)))


def _spread_timeseries(result: FactorAnalysisResult) -> go.Figure:
    periods = list(result.periods)
    figure = make_subplots(
        rows=len(periods),
        cols=1,
        shared_xaxes=True,
        vertical_spacing=min(0.12, 0.2 / max(1, len(periods))),
        subplot_titles=[f"{period}日 Top − Bottom 日均收益" for period in periods],
    )
    for row, period in enumerate(periods, start=1):
        group = result.daily_metrics[
            result.daily_metrics["period"] == period
        ].sort_values("date")
        spread = pd.Series(
            _dailyize(group["long_short_return"], period) * 10_000.0,
            index=group.index,
        )
        rolling = spread.rolling(21, min_periods=5).mean()
        figure.add_trace(
            go.Scatter(
                x=group["date"],
                y=spread,
                name=f"{period}日价差",
                line={"color": "rgba(15,107,120,.28)", "width": 0.8},
                showlegend=row == 1,
            ),
            row=row,
            col=1,
        )
        figure.add_trace(
            go.Scatter(
                x=group["date"],
                y=rolling,
                name="21日移动平均",
                line={"color": ROLLING_COLOR, "width": 2.0},
                showlegend=row == 1,
            ),
            row=row,
            col=1,
        )
        figure.add_hline(
            y=0.0,
            line={"color": COLORS["muted"], "width": 1},
            row=row,
            col=1,
        )
        figure.update_yaxes(title="bps", row=row, col=1)
    figure.update_layout(title="头尾分位收益差")
    return _finish(figure, height=max(500, 260 * len(periods)))


def _information_timeseries(
    result: FactorAnalysisResult,
    column: str,
    label: str,
) -> go.Figure:
    periods = list(result.periods)
    figure = make_subplots(
        rows=len(periods),
        cols=1,
        shared_xaxes=True,
        vertical_spacing=min(0.12, 0.2 / max(1, len(periods))),
        subplot_titles=[f"{period}日 {label}" for period in periods],
    )
    for row, period in enumerate(periods, start=1):
        group = result.daily_metrics[
            result.daily_metrics["period"] == period
        ].sort_values("date")
        rolling = group[column].rolling(21, min_periods=5).mean()
        figure.add_trace(
            go.Scatter(
                x=group["date"],
                y=group[column],
                name=label,
                line={"color": "rgba(15,107,120,.38)", "width": 0.9},
                showlegend=row == 1,
            ),
            row=row,
            col=1,
        )
        figure.add_trace(
            go.Scatter(
                x=group["date"],
                y=rolling,
                name="21日移动平均",
                line={"color": ROLLING_COLOR, "width": 2.0},
                showlegend=row == 1,
            ),
            row=row,
            col=1,
        )
        figure.add_hline(
            y=0.0,
            line={"color": COLORS["muted"], "width": 1},
            row=row,
            col=1,
        )
    figure.update_layout(title=f"每日 {label} 与21日移动平均")
    return _finish(figure, height=max(500, 260 * len(periods)))


def _cumulative_information(result: FactorAnalysisResult) -> go.Figure:
    figure = go.Figure()
    for index, (period, group) in enumerate(
        result.daily_metrics.groupby("period", sort=False)
    ):
        ordered = group.sort_values("date")
        color = PERIOD_COLORS[index % len(PERIOD_COLORS)]
        figure.add_trace(
            go.Scatter(
                x=ordered["date"],
                y=ordered["ic"].fillna(0.0).cumsum(),
                name=f"IC · {int(period)}日",
                line={"color": color, "width": 2},
            )
        )
        figure.add_trace(
            go.Scatter(
                x=ordered["date"],
                y=ordered["rank_ic"].fillna(0.0).cumsum(),
                name=f"RankIC · {int(period)}日",
                line={"color": color, "dash": "dot", "width": 1.5},
            )
        )
    figure.update_layout(title="累计 IC / RankIC")
    return _finish(figure)


def _kde(values: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    values = values[np.isfinite(values)]
    if len(values) < 2 or float(np.std(values, ddof=1)) == 0.0:
        return np.array([]), np.array([])
    standard_deviation = float(np.std(values, ddof=1))
    bandwidth = max(1.06 * standard_deviation * len(values) ** (-0.2), 1e-8)
    grid = np.linspace(values.min() - bandwidth, values.max() + bandwidth, 180)
    z = (grid[:, None] - values[None, :]) / bandwidth
    density = np.exp(-0.5 * z * z).mean(axis=1) / (bandwidth * np.sqrt(2.0 * np.pi))
    return grid, density


def _ic_diagnostics(result: FactorAnalysisResult) -> go.Figure:
    periods = list(result.periods)
    titles = [
        title
        for period in periods
        for title in (f"{period}日 IC 分布", f"{period}日 IC 正态 Q-Q")
    ]
    figure = make_subplots(
        rows=len(periods),
        cols=2,
        horizontal_spacing=0.1,
        vertical_spacing=min(0.16, 0.32 / max(1, len(periods))),
        subplot_titles=titles,
    )
    normal = NormalDist()
    for row, period in enumerate(periods, start=1):
        values = (
            result.daily_metrics.loc[result.daily_metrics["period"] == period, "ic"]
            .dropna()
            .to_numpy(dtype=float)
        )
        figure.add_trace(
            go.Histogram(
                x=values,
                histnorm="probability density",
                nbinsx=45,
                name="IC",
                marker_color="rgba(15,107,120,.38)",
                showlegend=False,
            ),
            row=row,
            col=1,
        )
        grid, density = _kde(values)
        if len(grid):
            figure.add_trace(
                go.Scatter(
                    x=grid,
                    y=density,
                    name="KDE",
                    line={"color": ROLLING_COLOR, "width": 2},
                    showlegend=row == 1,
                ),
                row=row,
                col=1,
            )
        if len(values) > 1 and float(np.std(values, ddof=1)) > 0.0:
            observed = np.sort(
                (values - float(np.mean(values))) / float(np.std(values, ddof=1))
            )
            expected = np.array(
                [
                    normal.inv_cdf((index + 0.5) / len(values))
                    for index in range(len(values))
                ]
            )
            limits = [
                min(expected.min(), observed.min()),
                max(expected.max(), observed.max()),
            ]
            figure.add_trace(
                go.Scattergl(
                    x=expected,
                    y=observed,
                    mode="markers",
                    marker={
                        "color": COLORS["accent"],
                        "size": 4,
                        "opacity": 0.55,
                    },
                    name="样本分位",
                    showlegend=False,
                ),
                row=row,
                col=2,
            )
            figure.add_trace(
                go.Scatter(
                    x=limits,
                    y=limits,
                    mode="lines",
                    line={"color": COLORS["danger"], "width": 1.4},
                    name="正态参考线",
                    showlegend=row == 1,
                ),
                row=row,
                col=2,
            )
        if row == len(periods):
            figure.update_xaxes(title="IC", row=row, col=1)
            figure.update_xaxes(title="正态理论分位", row=row, col=2)
        figure.update_yaxes(title="样本标准化分位", row=row, col=2)
    for annotation in figure.layout.annotations:
        annotation.update(font={"size": 14})
    figure.update_layout(title="IC 分布与正态性诊断", barmode="overlay")
    figure = _finish(figure, height=max(680, 370 * len(periods)))
    figure.update_layout(
        margin={"l": 70, "r": 35, "t": 105, "b": 65},
        legend={
            "orientation": "h",
            "x": 1,
            "xanchor": "right",
            "y": 1.04,
            "yanchor": "bottom",
        },
    )
    figure.update_xaxes(automargin=True)
    figure.update_yaxes(automargin=True)
    return figure


def _monthly_ic_heatmaps(result: FactorAnalysisResult) -> go.Figure:
    periods = list(result.periods)
    figure = make_subplots(
        rows=1,
        cols=len(periods),
        horizontal_spacing=0.06,
        subplot_titles=[f"{period}日月均 IC" for period in periods],
    )
    for column, period in enumerate(periods, start=1):
        group = result.daily_metrics[result.daily_metrics["period"] == period].copy()
        group["date"] = pd.to_datetime(group["date"])
        group["year"] = group["date"].dt.year
        group["month"] = group["date"].dt.month
        heatmap = group.pivot_table(
            index="year", columns="month", values="ic", aggfunc="mean"
        ).reindex(columns=range(1, 13))
        text = np.where(
            heatmap.notna(),
            np.vectorize(lambda value: f"{value:.3f}")(heatmap.fillna(0.0)),
            "",
        )
        figure.add_trace(
            go.Heatmap(
                z=heatmap.to_numpy(dtype=float),
                x=list(range(1, 13)),
                y=[str(year) for year in heatmap.index],
                text=text,
                texttemplate="%{text}",
                textfont={"size": 9},
                colorscale=[
                    [0, COLORS["danger"]],
                    [0.5, "#ffffff"],
                    [1, COLORS["success"]],
                ],
                zmid=0.0,
                showscale=column == len(periods),
                colorbar={"title": "IC"},
                hovertemplate="%{y}年%{x}月<br>IC %{z:.4f}<extra></extra>",
            ),
            row=1,
            col=column,
        )
        figure.update_xaxes(title="月份", dtick=1, row=1, col=column)
    figure.update_layout(title="月度平均 IC 热力图")
    return _finish(figure, height=480)


def _turnover_chart(result: FactorAnalysisResult) -> go.Figure:
    group = (
        result.daily_metrics.sort_values("date")
        .drop_duplicates(["date", "factor"])
        .copy()
    )
    figure = go.Figure()
    figure.add_trace(
        go.Scatter(
            x=group["date"],
            y=group["top_quantile_turnover"],
            name="头部分位换手",
            line={"color": PERIOD_COLORS[0], "width": 1.1},
        )
    )
    figure.add_trace(
        go.Scatter(
            x=group["date"],
            y=group["bottom_quantile_turnover"],
            name="底部分位换手",
            line={"color": PERIOD_COLORS[1], "width": 1.1},
        )
    )
    figure.update_layout(title="头部与底部分位每日换手")
    figure.update_yaxes(title="新进入成分占比", tickformat=".0%", range=[0, 1])
    return _finish(figure)


def _rank_autocorrelation_chart(result: FactorAnalysisResult) -> go.Figure:
    group = result.daily_metrics.sort_values("date").drop_duplicates(["date", "factor"])
    mean_value = float(group["factor_rank_autocorrelation"].mean())
    figure = go.Figure(
        go.Scatter(
            x=group["date"],
            y=group["factor_rank_autocorrelation"],
            name="秩自相关",
            line={"color": PERIOD_COLORS[0], "width": 1.2},
        )
    )
    figure.add_hline(
        y=mean_value,
        line={"color": ROLLING_COLOR, "dash": "dash"},
        annotation_text=f"均值 {mean_value:.3f}",
    )
    figure.update_layout(title="因子秩自相关")
    figure.update_yaxes(title="相关系数", range=[-1, 1])
    return _finish(figure)


def _sector_ic_chart(result: FactorAnalysisResult) -> go.Figure:
    if result.sector_ic.empty:
        return empty_figure("行业 IC", "没有可用的历史行业数据")
    sector_order = (
        result.sector_ic.groupby("sector", sort=False)["mean_ic"]
        .mean()
        .sort_values()
        .index.astype(str)
        .tolist()
    )
    figure = go.Figure()
    for index, (period, group) in enumerate(
        result.sector_ic.groupby("period", sort=True)
    ):
        ordered = group.assign(sector=group["sector"].astype(str)).set_index("sector")
        ordered = ordered.reindex(sector_order)
        figure.add_trace(
            go.Bar(
                x=ordered["mean_ic"],
                y=sector_order,
                orientation="h",
                name=f"{int(period)}日",
                marker_color=PERIOD_COLORS[index % len(PERIOD_COLORS)],
                hovertemplate="%{y}<br>IC %{x:.4f}<extra></extra>",
            )
        )
    figure.add_vline(x=0.0, line={"color": COLORS["muted"], "dash": "dot"})
    figure.update_layout(
        title="分行业信息系数",
        barmode="group",
        bargap=0.18,
        bargroupgap=0.04,
    )
    figure.update_xaxes(title="IC", automargin=True)
    figure.update_yaxes(automargin=True)
    height = max(600, 30 * len(sector_order) + 150)
    left_margin = max(90, min(200, 35 + 12 * max(map(len, sector_order))))
    figure = _finish(figure, height=height)
    figure.update_layout(
        margin={"l": left_margin, "r": 35, "t": 90, "b": 55},
        legend={
            "orientation": "h",
            "x": 1,
            "xanchor": "right",
            "y": 1.03,
            "yanchor": "bottom",
        },
    )
    return figure


def _sector_quantile_chart(result: FactorAnalysisResult) -> go.Figure:
    data = result.sector_quantile_returns
    if data.empty:
        return empty_figure("行业内分位收益", "没有可用的历史行业数据")
    sectors = sorted(data["sector"].dropna().astype(str).unique())
    columns = 2
    rows = int(np.ceil(len(sectors) / columns))
    figure = make_subplots(
        rows=rows,
        cols=columns,
        subplot_titles=sectors,
        horizontal_spacing=0.09,
        vertical_spacing=min(0.08, 0.28 / max(1, rows)),
    )
    periods = sorted(data["period"].unique())
    for sector_index, sector in enumerate(sectors):
        row, column = divmod(sector_index, columns)
        row += 1
        column += 1
        sector_data = data[data["sector"].astype(str) == sector]
        for period_index, period in enumerate(periods):
            group = sector_data[sector_data["period"] == period].sort_values("quantile")
            figure.add_trace(
                go.Bar(
                    x=[f"Q{int(value)}" for value in group["quantile"]],
                    y=_dailyize(group["mean_return"], int(period)) * 10_000.0,
                    name=f"{int(period)}日",
                    marker_color=PERIOD_COLORS[period_index % len(PERIOD_COLORS)],
                    showlegend=sector_index == 0,
                    hovertemplate="%{x}<br>日均收益 %{y:.3f} bps<extra></extra>",
                ),
                row=row,
                col=column,
            )
        figure.add_hline(
            y=0.0,
            line={"color": COLORS["muted"], "width": 1},
            row=row,
            col=column,
        )
        figure.update_yaxes(title="bps", row=row, col=column)
    figure.update_layout(title="分行业的因子分位平均日收益", barmode="group")
    return _finish(figure, height=max(520, 275 * rows))


def _table(headers: list[str], rows: list[tuple[str, list[str]]]) -> str:
    header = "".join(f"<th>{escape(value)}</th>" for value in headers)
    body = "".join(
        "<tr>"
        f'<th scope="row">{escape(label)}</th>'
        + "".join(f"<td>{value}</td>" for value in values)
        + "</tr>"
        for label, values in rows
    )
    return f'<div class="table-wrap"><table><thead><tr><th>指标</th>{header}</tr></thead><tbody>{body}</tbody></table></div>'


def _returns_table(result: FactorAnalysisResult) -> str:
    values = result.return_analysis.set_index("period")
    periods = list(result.periods)
    specs = [
        ("年化 Alpha", "annual_alpha", lambda value: percent(value)),
        ("t-stat(Alpha)", "alpha_t_stat", lambda value: number(value, 3)),
        ("Beta", "beta", lambda value: number(value, 3)),
        (
            "头部分位平均日收益 (bps)",
            "mean_daily_top_bps",
            lambda value: number(value, 3),
        ),
        (
            "底部分位平均日收益 (bps)",
            "mean_daily_bottom_bps",
            lambda value: number(value, 3),
        ),
        (
            "头尾平均日收益差 (bps)",
            "mean_daily_spread_bps",
            lambda value: number(value, 3),
        ),
    ]
    rows = [
        (label, [formatter(values.loc[period, column]) for period in periods])
        for label, column, formatter in specs
    ]
    return _table([f"{period}日" for period in periods], rows)


def _information_table(result: FactorAnalysisResult) -> str:
    values = result.summary_metrics.set_index("period")
    periods = list(result.periods)
    specs = [
        ("IC 均值", "mean_ic", lambda value: number(value, 4)),
        ("IC 标准差", "ic_std", lambda value: number(value, 4)),
        ("t-stat(IC)", "ic_t_stat", lambda value: number(value, 3)),
        ("p-value(IC)", "ic_p_value", lambda value: number(value, 4)),
        ("IC 偏度", "ic_skew", lambda value: number(value, 3)),
        ("IC 峰度", "ic_kurtosis", lambda value: number(value, 3)),
        ("年化 IR", "annualized_icir", lambda value: number(value, 3)),
        ("RankIC 均值", "mean_rank_ic", lambda value: number(value, 4)),
        ("RankICIR", "rank_icir", lambda value: number(value, 3)),
        ("RankIC 正值占比", "rank_ic_positive_rate", lambda value: percent(value)),
    ]
    rows = [
        (label, [formatter(values.loc[period, column]) for period in periods])
        for label, column, formatter in specs
    ]
    return _table([f"{period}日" for period in periods], rows)


def _turnover_table(result: FactorAnalysisResult) -> str:
    row = result.summary_metrics.iloc[0]
    return _table(
        ["头部分位", "底部分位", "因子秩自相关"],
        [
            (
                "均值",
                [
                    percent(row.mean_top_quantile_turnover),
                    percent(row.mean_bottom_quantile_turnover),
                    number(row.mean_factor_rank_autocorrelation, 3),
                ],
            )
        ],
    )


def _neutralization_label(result: FactorAnalysisResult) -> str:
    labels = []
    if result.metadata.get("industry_neutral"):
        labels.append("行业")
    if result.metadata.get("market_cap_neutral"):
        labels.append("市值")
    return " + ".join(labels) if labels else "无"


def _core_metric_cards(result: FactorAnalysisResult) -> str:
    metrics = result.summary_metrics.set_index("period")
    cards = []
    for period in result.periods:
        row = metrics.loc[period]
        for label, column, digits in (
            ("IC 均值", "mean_ic", 4),
            ("RankIC 均值", "mean_rank_ic", 4),
            ("年化 ICIR", "annualized_icir", 3),
        ):
            value = float(row[column])
            css_class = ""
            if np.isfinite(value):
                css_class = "positive" if value >= 0 else "negative"
            cards.append(
                metric_card(
                    f"{period}日 {label}",
                    number(value, digits),
                    css_class,
                )
            )
    return "".join(cards)


def generate_factor_report(
    result: FactorAnalysisResult,
    path: str | Path,
) -> Path:
    """Write a responsive single-factor report containing all sample analyses."""

    if len(result.factors) != 1:
        raise ValueError(
            "a factor report requires exactly one factor; export each factor separately"
        )
    factor_name = result.factors[0]
    expression = result.factor_expressions.get(factor_name, factor_name)
    figures = figure_html(
        [
            _mean_return_by_quantile(result),
            _quantile_distribution(result),
            _factor_weighted_cumulative(result),
            _quantile_cumulative(result),
            _spread_timeseries(result),
            _information_timeseries(result, "ic", "IC"),
            _information_timeseries(result, "rank_ic", "RankIC"),
            _cumulative_information(result),
            _ic_diagnostics(result),
            _monthly_ic_heatmaps(result),
            _turnover_chart(result),
            _rank_autocorrelation_chart(result),
            _sector_ic_chart(result),
            _sector_quantile_chart(result),
        ],
        result.plotly_js,
    )
    index_code = result.metadata.get("index_code")
    universe_name = result.metadata.get("index_name") or (
        "指数资产池" if index_code else "固定资产池"
    )
    universe_target = index_code or "自定义标的"
    filters = (
        ("ST", "剔除" if result.metadata.get("exclude_st") else "保留"),
        ("停牌", "剔除" if result.metadata.get("exclude_suspended") else "保留"),
        ("上市", f"≥{result.metadata.get('min_listed_days', 0)}天"),
    )
    filter_html = "".join(
        f'<span class="summary-line">{escape(label)}：{escape(value)}</span>'
        for label, value in filters
    )
    generated_at = datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y-%m-%d %H:%M:%S")
    html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{escape(result.title)}</title>
<style>
:root{{--primary:#2c3e50;--accent:#3498db;--bg:#f5f7fa;--card:#fff;--muted:#7f8c8d;--border:#e1e4e8;--success:#27ae60;--danger:#c0392b}}
*{{box-sizing:border-box}} body{{margin:0;padding:20px;background:var(--bg);color:#333;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Arial,"Microsoft YaHei",sans-serif;line-height:1.55}}
.container{{max-width:1200px;margin:auto;background:var(--card);padding:38px;border-radius:12px;box-shadow:0 4px 14px rgba(0,0,0,.06)}}
header{{text-align:center;border-bottom:1px solid var(--border);padding-bottom:20px;margin-bottom:28px}} h1{{margin:0;color:var(--primary);font-size:28px}} header p,.muted{{color:var(--muted)}}
.summary{{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:16px;background:#f8f9fa;border:1px solid var(--border);padding:18px;border-radius:8px}}
.summary-label,.metric-label{{font-size:13px;color:var(--muted)}} .summary-value{{font-size:17px;font-weight:650;color:var(--primary);margin-top:4px;overflow-wrap:anywhere}}
.summary-line{{display:block}}
.section-title{{font-size:20px;font-weight:650;color:var(--primary);margin:38px 0 18px;padding-left:12px;border-left:4px solid var(--accent)}}
.metrics-grid,.analysis-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:16px}}
.metric-card{{background:#fff;border:1px solid var(--border);border-radius:8px;padding:17px;text-align:center}} .metric-value{{font-size:24px;font-weight:700;color:var(--primary)}} .positive{{color:var(--success)}} .negative{{color:var(--danger)}}
.formula{{background:#f8f9fa;border:1px solid var(--border);border-radius:8px;padding:18px}} .formula code{{display:block;margin-top:5px;color:var(--primary);font-family:"Cascadia Code",Consolas,monospace;font-size:17px;font-weight:650;overflow-wrap:anywhere}}
.chart{{border:1px solid var(--border);border-radius:8px;padding:4px;margin-bottom:18px;overflow:hidden}} .row{{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:18px}}
.table-wrap{{overflow:auto;border:1px solid var(--border);border-radius:8px}} table{{width:100%;border-collapse:collapse;font-size:14px}} th,td{{padding:10px 12px;border-bottom:1px solid var(--border);text-align:left;white-space:nowrap}} th{{background:#f8f9fa;color:var(--primary)}} tbody tr:last-child th,tbody tr:last-child td{{border-bottom:0}}
.tables h3,.turnover-table h3{{font-size:16px;color:var(--primary);margin:0 0 10px}} .turnover-table{{margin-top:18px;max-width:720px}}
footer{{text-align:center;margin-top:45px;padding-top:18px;border-top:1px solid var(--border);font-size:12px;color:var(--muted)}}
@media(max-width:800px){{.container{{padding:18px}}.row{{grid-template-columns:1fr}}body{{padding:8px}}h1{{overflow-wrap:anywhere}}}}
</style>
</head>
<body><div class="container">
<header><h1>{escape(result.title)}</h1><p>{escape(factor_name)} · 生成时间：{generated_at}</p></header>
<div class="summary">
<div><div class="summary-label">分析区间</div><div class="summary-value">{result.start.date()} ~ {result.end.date()}</div></div>
<div><div class="summary-label">预测周期</div><div class="summary-value">{escape(" / ".join(f"{period}日" for period in result.periods))}</div></div>
<div><div class="summary-label">资产池</div><div class="summary-value"><span class="summary-line">{escape(str(universe_name))}</span><span class="summary-line">标的：{escape(str(universe_target))}</span></div></div>
<div><div class="summary-label">样本过滤</div><div class="summary-value">{filter_html}</div></div>
<div><div class="summary-label">中性化</div><div class="summary-value">{escape(_neutralization_label(result))}</div></div>
</div>

<div class="section-title">因子公式 (Factor Expression)</div>
<div class="formula"><code>{escape(expression)}</code></div>

<div class="section-title">核心指标 (Key Metrics)</div>
<div class="metrics-grid">{_core_metric_cards(result)}</div>

<div class="section-title">汇总分析 (Summary Analysis)</div>
<div class="row tables"><div><h3>Returns Analysis</h3>{_returns_table(result)}</div><div><h3>Information Analysis</h3>{_information_table(result)}</div></div>
<div class="turnover-table"><h3>Turnover Analysis</h3>{_turnover_table(result)}</div>

<div class="section-title">收益分析 (Return Analysis)</div>
<div class="row"><div class="chart">{figures[0]}</div><div class="chart">{figures[1]}</div></div>
<div class="chart">{figures[2]}</div><div class="chart">{figures[3]}</div>
<div class="chart">{figures[4]}</div>

<div class="section-title">信息分析 (Information Analysis)</div>
<div class="chart">{figures[5]}</div><div class="chart">{figures[6]}</div><div class="chart">{figures[7]}</div><div class="chart" id="ic-diagnostics">{figures[8]}</div><div class="chart">{figures[9]}</div>

<div class="section-title">换手与稳定性 (Turnover Analysis)</div>
<div class="chart">{figures[10]}</div><div class="chart">{figures[11]}</div>

<div class="section-title">行业分析 (Sector Analysis)</div>
<div class="chart" id="sector-ic">{figures[12]}</div><div class="chart">{figures[13]}</div>
<footer>TuAlpha Report · Powered by Plotly · 单因子分析报告</footer>
</div></body></html>"""
    destination = Path(path)
    destination.write_text(html, encoding="utf-8")
    return destination


__all__ = ["generate_factor_report"]
