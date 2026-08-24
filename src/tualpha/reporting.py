"""Chinese single-file Plotly report generation."""

from __future__ import annotations

from datetime import datetime
from html import escape
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio
from plotly.subplots import make_subplots

from .config import PlotlyJsMode
from .result import BacktestResult

_COLORS = {
    "primary": "#2c3e50",
    "accent": "#3498db",
    "success": "#27ae60",
    "danger": "#c0392b",
    "muted": "#7f8c8d",
    "grid": "#e8ecf1",
}
_MONTHS = [
    "1月",
    "2月",
    "3月",
    "4月",
    "5月",
    "6月",
    "7月",
    "8月",
    "9月",
    "10月",
    "11月",
    "12月",
]
_REJECTION_LABELS = {
    "limit_up": "涨停禁止买入",
    "limit_down": "跌停禁止卖出",
    "suspended": "停牌",
    "no_market_data": "无行情",
    "zero_volume": "零成交量",
    "invalid_lot": "交易单位不合法",
    "t_plus_one": "T+1 可卖不足",
    "insufficient_cash": "现金不足",
    "insufficient_position": "持仓不足",
    "asset_not_alive": "未上市/已退市",
    "end_of_backtest": "回测结束",
    "zero_amount": "零数量",
}


def _finite(value: Any) -> bool:
    try:
        return bool(np.isfinite(float(value)))
    except (TypeError, ValueError):
        return False


def _percent(value: Any) -> str:
    return f"{float(value):.2%}" if _finite(value) else "--"


def _number(value: Any, digits: int = 2) -> str:
    return f"{float(value):.{digits}f}" if _finite(value) else "--"


def _money(value: Any) -> str:
    if not _finite(value):
        return "--"
    amount = float(value)
    absolute = abs(amount)
    if absolute >= 100_000_000:
        return f"{amount / 100_000_000:.2f}亿"
    if absolute >= 10_000:
        return f"{amount / 10_000:.2f}万"
    return f"{amount:,.2f}"


def _empty_figure(title: str, message: str) -> go.Figure:
    figure = go.Figure()
    figure.add_annotation(
        text=message,
        x=0.5,
        y=0.5,
        xref="paper",
        yref="paper",
        showarrow=False,
        font={"color": _COLORS["muted"], "size": 15},
    )
    figure.update_layout(title=title, height=360)
    return figure


def _base_layout(figure: go.Figure, height: int = 420) -> go.Figure:
    figure.update_layout(
        template="plotly_white",
        height=height,
        margin={"l": 55, "r": 30, "t": 60, "b": 45},
        font={
            "family": "Arial, Microsoft YaHei, sans-serif",
            "color": _COLORS["primary"],
        },
        hovermode="x unified",
        paper_bgcolor="white",
        plot_bgcolor="white",
        legend={"orientation": "h", "y": 1.03, "x": 0},
    )
    figure.update_xaxes(gridcolor=_COLORS["grid"])
    figure.update_yaxes(gridcolor=_COLORS["grid"])
    return figure


def _dashboard(performance: pd.DataFrame) -> go.Figure:
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
            line={"color": _COLORS["accent"], "width": 2},
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
            line={"color": _COLORS["danger"], "width": 1},
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
                x=_MONTHS,
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
    return _base_layout(figure, height=850)


def _yearly_returns(performance: pd.DataFrame) -> go.Figure:
    yearly = (1.0 + performance["returns"].fillna(0.0)).resample("YE").prod() - 1.0
    colors = [
        _COLORS["success"] if value >= 0 else _COLORS["danger"] for value in yearly
    ]
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
    return _base_layout(figure)


def _daily_distribution(performance: pd.DataFrame) -> go.Figure:
    returns = performance["returns"].dropna()
    figure = go.Figure(
        go.Histogram(
            x=returns,
            nbinsx=min(60, max(10, int(np.sqrt(max(1, len(returns)))) * 2)),
            marker_color=_COLORS["accent"],
            opacity=0.8,
        )
    )
    figure.update_layout(title="日收益率分布 (Daily Returns Distribution)")
    figure.update_xaxes(title="收益率", tickformat=".2%")
    figure.update_yaxes(title="频次")
    return _base_layout(figure)


def _rolling_risk(performance: pd.DataFrame, annualization: int) -> go.Figure:
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
            line={"color": _COLORS["accent"]},
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
    return _base_layout(figure, height=520)


def _benchmark_chart(result: BacktestResult) -> go.Figure:
    performance = result.performance
    if "benchmark_returns" not in performance:
        return _empty_figure("策略与基准累计收益对比", "未配置基准")
    strategy = (1.0 + performance["returns"].fillna(0.0)).cumprod() - 1.0
    benchmark = (1.0 + performance["benchmark_returns"].fillna(0.0)).cumprod() - 1.0
    figure = go.Figure()
    figure.add_trace(
        go.Scatter(
            x=strategy.index,
            y=strategy,
            name="策略",
            line={"color": _COLORS["accent"], "width": 2},
        )
    )
    figure.add_trace(
        go.Scatter(
            x=benchmark.index,
            y=benchmark,
            name=result.config.benchmark or "基准",
            line={"color": _COLORS["muted"], "width": 2},
        )
    )
    figure.update_layout(title="策略与基准累计收益对比 (Cumulative Return Comparison)")
    figure.update_yaxes(tickformat=".1%")
    return _base_layout(figure)


def _trade_distribution(trades: pd.DataFrame) -> go.Figure:
    if trades.empty:
        return _empty_figure(
            "交易盈亏分布 (Trade PnL Distribution)", "回测期间没有已平仓交易"
        )
    figure = go.Figure(
        go.Histogram(x=trades["pnl"], nbinsx=40, marker_color=_COLORS["accent"])
    )
    figure.update_layout(title="交易盈亏分布 (Trade PnL Distribution)")
    figure.update_xaxes(title="盈亏")
    figure.update_yaxes(title="频次")
    return _base_layout(figure)


def _trade_duration(trades: pd.DataFrame) -> go.Figure:
    if trades.empty:
        return _empty_figure(
            "盈亏 vs 持仓时间 (PnL vs Duration)", "回测期间没有已平仓交易"
        )
    colors = [
        _COLORS["success"] if value >= 0 else _COLORS["danger"]
        for value in trades["pnl"]
    ]
    figure = go.Figure(
        go.Scatter(
            x=trades["holding_days"],
            y=trades["pnl"],
            mode="markers",
            text=trades["ts_code"],
            marker={"color": colors, "size": 8, "opacity": 0.75},
            hovertemplate="%{text}<br>持仓 %{x} 天<br>盈亏 %{y:,.2f}<extra></extra>",
        )
    )
    figure.update_layout(title="盈亏 vs 持仓时间 (PnL vs Duration)")
    figure.update_xaxes(title="持仓时间（天）")
    figure.update_yaxes(title="盈亏")
    return _base_layout(figure)


def _metric_card(label: str, value: str, css_class: str = "") -> str:
    return (
        '<div class="metric-card">'
        f'<div class="metric-value {css_class}">{escape(value)}</div>'
        f'<div class="metric-label">{escape(label)}</div>'
        "</div>"
    )


def _attribution_rows(result: BacktestResult) -> str:
    trades = result.closed_trades
    transactions = result.transactions
    positions = result.daily_positions
    holding_days = pd.Series(dtype=int)
    position_columns = {"record_type", "ts_code", "date", "quantity"}
    if not positions.empty and position_columns.issubset(positions.columns):
        held = positions[
            positions["record_type"].eq("POSITION")
            & pd.to_numeric(positions["quantity"], errors="coerce").gt(0)
        ]
        holding_days = held.groupby("ts_code")["date"].nunique()
    if trades.empty and transactions.empty and holding_days.empty:
        return '<tr><td colspan="6" class="muted">无交易归因数据</td></tr>'
    pnl = (
        trades.groupby("ts_code")["pnl"].sum()
        if not trades.empty
        else pd.Series(dtype=float)
    )
    counts = (
        transactions.groupby("ts_code").size()
        if not transactions.empty
        else pd.Series(dtype=int)
    )
    fees = (
        transactions.groupby("ts_code")["fees"].sum()
        if not transactions.empty
        else pd.Series(dtype=float)
    )
    codes = pnl.index.union(counts.index).union(fees.index).union(holding_days.index)
    total_pnl = float(pnl.sum())
    rows = []
    for code in sorted(codes, key=lambda item: float(pnl.get(item, 0.0)), reverse=True):
        value = float(pnl.get(code, 0.0))
        contribution = value / total_pnl if total_pnl else np.nan
        rows.append(
            "<tr>"
            f"<td>{escape(str(code))}</td>"
            f"<td>{int(counts.get(code, 0))}</td>"
            f"<td>{int(holding_days.get(code, 0))}</td>"
            f"<td>{_money(value)}</td>"
            f"<td>{_percent(contribution)}</td>"
            f"<td>{_money(fees.get(code, 0.0))}</td>"
            "</tr>"
        )
    return "".join(rows)


def _rejection_rows(result: BacktestResult) -> str:
    if result.orders.empty:
        return '<tr><td colspan="2" class="muted">无订单</td></tr>'
    rejected = result.orders[result.orders["reject_reason"].astype(str) != ""]
    if rejected.empty:
        return '<tr><td colspan="2" class="muted">无拒单或撤单</td></tr>'
    counts = rejected.groupby("reject_reason").size().sort_values(ascending=False)
    return "".join(
        f"<tr><td>{escape(_REJECTION_LABELS.get(str(reason), str(reason)))}</td><td>{int(count)}</td></tr>"
        for reason, count in counts.items()
    )


def _figure_html(figures: list[go.Figure], mode: PlotlyJsMode) -> list[str]:
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


def generate_html_report(result: BacktestResult, path: str | Path) -> Path:
    """Write a responsive report inspired by examples/example_report.html."""

    performance = result.performance
    metrics = result.metrics
    figures = _figure_html(
        [
            _dashboard(performance),
            _yearly_returns(performance),
            _daily_distribution(performance),
            _rolling_risk(performance, result.config.annualization_factor),
            _benchmark_chart(result),
            _trade_distribution(result.closed_trades),
            _trade_duration(result.closed_trades),
        ],
        result.config.plotly_js,
    )
    metric_specs = [
        (
            "累计收益率 (Total Return)",
            _percent(metrics.get("total_return")),
            "positive" if metrics.get("total_return", 0) >= 0 else "negative",
        ),
        (
            "年化收益率 (CAGR)",
            _percent(metrics.get("cagr")),
            "positive" if metrics.get("cagr", 0) >= 0 else "negative",
        ),
        ("夏普比率 (Sharpe)", _number(metrics.get("sharpe")), ""),
        ("索提诺比率 (Sortino)", _number(metrics.get("sortino")), ""),
        ("卡玛比率 (Calmar)", _number(metrics.get("calmar")), ""),
        ("最大回撤 (Max DD)", _percent(metrics.get("max_drawdown")), "negative"),
        ("波动率 (Volatility)", _percent(metrics.get("volatility")), ""),
        ("胜率 (Win Rate)", _percent(metrics.get("win_rate")), ""),
        ("盈亏比 (Profit Factor)", _number(metrics.get("profit_factor")), ""),
        ("已完成交易数", str(metrics.get("closed_trades", 0)), ""),
        ("未平仓标的数", str(metrics.get("open_positions", 0)), ""),
        ("总交易成本", _money(metrics.get("total_fees")), ""),
    ]
    cards = "".join(_metric_card(*spec) for spec in metric_specs)
    benchmark_metrics = ""
    if result.config.benchmark:
        benchmark_metrics = (
            '<div class="analysis-grid">'
            + _metric_card("基准", result.config.benchmark)
            + _metric_card("累计超额收益", _percent(metrics.get("total_excess_return")))
            + _metric_card("跟踪误差", _percent(metrics.get("tracking_error")))
            + _metric_card("信息比率", _number(metrics.get("information_ratio")))
            + _metric_card("Beta", _number(metrics.get("beta"), 4))
            + _metric_card("Alpha（年化）", _percent(metrics.get("alpha")))
            + "</div>"
        )

    fee_rows = "".join(
        [
            f"<tr><td>佣金</td><td>{_money(metrics.get('total_commission'))}</td></tr>",
            f"<tr><td>印花税</td><td>{_money(metrics.get('total_stamp_tax'))}</td></tr>",
            f"<tr><td>另计经手费</td><td>{_money(metrics.get('total_handling_fee'))}</td></tr>",
            f"<tr><td>过户费</td><td>{_money(metrics.get('total_transfer_fee'))}</td></tr>",
            f"<tr><td>佣金内含经手费（参考）</td><td>{_money(performance['included_handling_fee'].sum())}</td></tr>",
        ]
    )
    start = performance.index.min().date()
    end = performance.index.max().date()
    html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{escape(result.config.strategy_name)} 回测报告</title>
<style>
:root{{--primary:#2c3e50;--accent:#3498db;--bg:#f5f7fa;--card:#fff;--muted:#7f8c8d;--border:#e1e4e8;--success:#27ae60;--danger:#c0392b}}
*{{box-sizing:border-box}} body{{margin:0;padding:20px;background:var(--bg);color:#333;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Arial,"Microsoft YaHei",sans-serif;line-height:1.55}}
.container{{max-width:1200px;margin:auto;background:var(--card);padding:38px;border-radius:12px;box-shadow:0 4px 14px rgba(0,0,0,.06)}}
header{{text-align:center;border-bottom:1px solid var(--border);padding-bottom:20px;margin-bottom:28px}} h1{{margin:0;color:var(--primary);font-size:28px}} header p,.muted{{color:var(--muted)}}
.summary{{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:16px;background:#f8f9fa;border:1px solid var(--border);padding:18px;border-radius:8px}}
.summary-label,.metric-label{{font-size:13px;color:var(--muted)}} .summary-value{{font-size:17px;font-weight:650;color:var(--primary);margin-top:4px}}
.section-title{{font-size:20px;font-weight:650;color:var(--primary);margin:38px 0 18px;padding-left:12px;border-left:4px solid var(--accent)}}
.metrics-grid,.analysis-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:16px}}
.metric-card{{background:#fff;border:1px solid var(--border);border-radius:8px;padding:17px;text-align:center}} .metric-value{{font-size:24px;font-weight:700;color:var(--primary)}} .positive{{color:var(--success)}} .negative{{color:var(--danger)}}
.chart{{border:1px solid var(--border);border-radius:8px;padding:4px;margin-bottom:18px;overflow:hidden}} .row{{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:18px}}
.table-wrap{{overflow:auto;border:1px solid var(--border);border-radius:8px}} table{{width:100%;border-collapse:collapse;font-size:14px}} th,td{{padding:10px 12px;border-bottom:1px solid var(--border);text-align:left;white-space:nowrap}} th{{background:#f8f9fa;color:var(--primary)}}
footer{{text-align:center;margin-top:45px;padding-top:18px;border-top:1px solid var(--border);font-size:12px;color:var(--muted)}}
@media(max-width:800px){{.container{{padding:18px}}.row{{grid-template-columns:1fr}}body{{padding:8px}}}}
</style>
</head>
<body><div class="container">
<header><h1>{escape(result.config.strategy_name)} 回测报告</h1><p>生成时间：{datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y-%m-%d %H:%M:%S")}</p></header>
<div class="summary">
<div><div class="summary-label">回测区间</div><div class="summary-value">{start} ~ {end}</div></div>
<div><div class="summary-label">交易日数</div><div class="summary-value">{len(performance)}</div></div>
<div><div class="summary-label">初始资金</div><div class="summary-value">{result.config.capital_base:,.2f}</div></div>
<div><div class="summary-label">最终权益</div><div class="summary-value">{result.final_value:,.2f}</div></div>
<div><div class="summary-label">复权 / 成交</div><div class="summary-value">{result.config.adjustment.value} / next {result.config.execution_time.value}</div></div>
</div>
<div class="section-title">核心指标 (Key Metrics)</div><div class="metrics-grid">{cards}</div>
<div class="section-title">权益与回撤 (Equity & Drawdown)</div><div class="chart">{figures[0]}</div>
<div class="section-title">收益分析 (Return Analysis)</div><div class="row"><div class="chart">{figures[1]}</div><div class="chart">{figures[2]}</div></div><div class="chart">{figures[3]}</div>
<div class="section-title">基准对比 (Benchmark Comparison)</div>{benchmark_metrics}<div class="chart">{figures[4]}</div>
<div class="section-title">交易分析 (Trade Analysis)</div><div class="row"><div class="chart">{figures[5]}</div><div class="chart">{figures[6]}</div></div>
<div class="section-title">费用与交易限制 (Fees & Trading Constraints)</div><div class="row">
<div class="table-wrap"><table><thead><tr><th>费用项目</th><th>金额</th></tr></thead><tbody>{fee_rows}</tbody></table></div>
<div class="table-wrap"><table><thead><tr><th>拒单原因</th><th>次数</th></tr></thead><tbody>{_rejection_rows(result)}</tbody></table></div>
</div>
<div class="section-title">组合归因 (Attribution)</div><div class="table-wrap"><table><thead><tr><th>标的</th><th>成交次数</th><th>持有总天数</th><th>已实现盈亏</th><th>贡献占比</th><th>总费用</th></tr></thead><tbody>{_attribution_rows(result)}</tbody></table></div>
<footer>TuAlpha Report · Powered by Plotly · 股票与 ETF 日频回测</footer>
</div></body></html>"""
    destination = Path(path)
    destination.write_text(html, encoding="utf-8")
    return destination
