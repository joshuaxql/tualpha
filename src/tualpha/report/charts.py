"""Plotly chart construction and HTML embedding."""

from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio
from plotly.subplots import make_subplots

from ..analysis.result import BacktestResult
from ..foundation.config import PlotlyJsMode
from .formatting import COLORS, MONTHS


def empty_figure(title: str, message: str) -> go.Figure:
    figure = go.Figure()
    figure.add_annotation(
        text=message,
        x=0.5,
        y=0.5,
        xref="paper",
        yref="paper",
        showarrow=False,
        font={"color": COLORS["muted"], "size": 15},
    )
    figure.update_layout(title=title, height=360)
    return figure


def base_layout(figure: go.Figure, height: int = 420) -> go.Figure:
    figure.update_layout(
        template="plotly_white",
        height=height,
        margin={"l": 55, "r": 30, "t": 60, "b": 45},
        font={
            "family": "Arial, Microsoft YaHei, sans-serif",
            "color": COLORS["primary"],
        },
        hovermode="x unified",
        paper_bgcolor="white",
        plot_bgcolor="white",
        legend={"orientation": "h", "y": 1.03, "x": 0},
    )
    figure.update_xaxes(gridcolor=COLORS["grid"])
    figure.update_yaxes(gridcolor=COLORS["grid"])
    return figure


def dashboard(performance: pd.DataFrame) -> go.Figure:
    returns = performance["returns"].fillna(0.0)
    portfolio = performance["portfolio_value"]
    drawdown = portfolio / portfolio.cummax() - 1.0
    monthly = (1.0 + returns).resample("ME").prod() - 1.0
    monthly_frame = pd.DataFrame(
        {
            "year": monthly.index.year,
            "month": monthly.index.month,
            "return": monthly.values,
        }
    )
    heatmap = monthly_frame.pivot(
        index="year", columns="month", values="return"
    ).reindex(columns=range(1, 13))

    figure = make_subplots(
        rows=3,
        cols=1,
        row_heights=[0.45, 0.22, 0.33],
        vertical_spacing=0.09,
        subplot_titles=(
            "权益曲线 (Equity)",
            "回撤 (Drawdown)",
            "月度收益 (Monthly Returns)",
        ),
    )
    figure.add_trace(
        go.Scatter(
            x=portfolio.index,
            y=portfolio,
            name="组合权益",
            line={"color": COLORS["accent"], "width": 2},
        ),
        row=1,
        col=1,
    )
    figure.add_trace(
        go.Scatter(
            x=drawdown.index,
            y=drawdown,
            name="回撤",
            fill="tozeroy",
            line={"color": COLORS["danger"], "width": 1},
        ),
        row=2,
        col=1,
    )
    if not heatmap.empty:
        text = np.where(
            pd.isna(heatmap.values),
            "",
            np.vectorize(lambda x: f"{x:.1%}")(heatmap.fillna(0).values),
        )
        figure.add_trace(
            go.Heatmap(
                z=heatmap.values,
                x=MONTHS,
                y=[str(year) for year in heatmap.index],
                text=text,
                texttemplate="%{text}",
                colorscale=[[0, "#c0392b"], [0.5, "#ffffff"], [1, "#27ae60"]],
                zmid=0,
                colorbar={"title": "收益率"},
                hovertemplate="%{y} %{x}<br>%{z:.2%}<extra></extra>",
            ),
            row=3,
            col=1,
        )
    figure.update_yaxes(tickformat=",.0f", row=1, col=1)
    figure.update_yaxes(tickformat=".1%", row=2, col=1)
    figure.update_layout(title="策略仪表盘 (Strategy Dashboard)")
    return base_layout(figure, height=850)


def yearly_returns(performance: pd.DataFrame) -> go.Figure:
    yearly = (1.0 + performance["returns"].fillna(0.0)).resample("YE").prod() - 1.0
    colors = [COLORS["success"] if value >= 0 else COLORS["danger"] for value in yearly]
    figure = go.Figure(
        go.Bar(
            x=yearly.index.year,
            y=yearly,
            marker_color=colors,
            text=[f"{value:.2%}" for value in yearly],
            textposition="auto",
        )
    )
    figure.update_layout(title="年度收益 (Yearly Returns)")
    figure.update_yaxes(title="收益率", tickformat=".1%")
    return base_layout(figure)


def daily_distribution(performance: pd.DataFrame) -> go.Figure:
    returns = performance["returns"].dropna()
    figure = go.Figure(
        go.Histogram(
            x=returns,
            nbinsx=min(60, max(10, int(np.sqrt(max(1, len(returns)))) * 2)),
            marker_color=COLORS["accent"],
            opacity=0.8,
        )
    )
    figure.update_layout(title="日收益率分布 (Daily Returns Distribution)")
    figure.update_xaxes(title="收益率", tickformat=".2%")
    figure.update_yaxes(title="频次")
    return base_layout(figure)


def rolling_risk(performance: pd.DataFrame, annualization: int) -> go.Figure:
    returns = performance["returns"].fillna(0.0)
    window = min(126, max(20, len(returns) // 4))
    rolling_std = returns.rolling(window).std()
    rolling_sharpe = (
        returns.rolling(window).mean() / rolling_std * np.sqrt(annualization)
    )
    rolling_volatility = rolling_std * np.sqrt(annualization)
    figure = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.12,
        subplot_titles=(f"滚动夏普比率 ({window} 天)", f"滚动波动率 ({window} 天)"),
    )
    figure.add_trace(
        go.Scatter(
            x=returns.index,
            y=rolling_sharpe,
            name="滚动夏普",
            line={"color": COLORS["accent"]},
        ),
        row=1,
        col=1,
    )
    figure.add_trace(
        go.Scatter(
            x=returns.index,
            y=rolling_volatility,
            name="滚动波动率",
            line={"color": "#8e44ad"},
        ),
        row=2,
        col=1,
    )
    figure.update_yaxes(title="夏普比率", row=1, col=1)
    figure.update_yaxes(title="波动率", tickformat=".1%", row=2, col=1)
    return base_layout(figure, height=520)


def benchmark_chart(result: BacktestResult) -> go.Figure:
    performance = result.performance
    if "benchmark_returns" not in performance:
        return empty_figure("策略与基准累计收益对比", "未配置基准")
    strategy = (1.0 + performance["returns"].fillna(0.0)).cumprod() - 1.0
    benchmark = (1.0 + performance["benchmark_returns"].fillna(0.0)).cumprod() - 1.0
    figure = go.Figure()
    figure.add_trace(
        go.Scatter(
            x=strategy.index,
            y=strategy,
            name="策略",
            line={"color": COLORS["accent"], "width": 2},
        )
    )
    figure.add_trace(
        go.Scatter(
            x=benchmark.index,
            y=benchmark,
            name=result.config.benchmark or "基准",
            line={"color": COLORS["muted"], "width": 2},
        )
    )
    figure.update_layout(title="策略与基准累计收益对比 (Cumulative Return Comparison)")
    figure.update_yaxes(tickformat=".1%")
    return base_layout(figure)


def figure_html(figures: list[go.Figure], mode: PlotlyJsMode) -> list[str]:
    include_first: bool | str = True if mode is PlotlyJsMode.INLINE else "cdn"
    blocks = []
    for index, figure in enumerate(figures):
        blocks.append(
            pio.to_html(
                figure,
                full_html=False,
                include_plotlyjs=include_first if index == 0 else False,
                config={"responsive": True, "displaylogo": False},
            )
        )
    return blocks
