"""Single-file Chinese HTML report assembly."""

from __future__ import annotations

from datetime import datetime
from html import escape
from pathlib import Path
from zoneinfo import ZoneInfo

from ..analysis.result import BacktestResult
from .attribution import attribution_rows, rejection_rows
from .charts import (
    benchmark_chart,
    daily_distribution,
    dashboard,
    figure_html,
    rolling_risk,
    yearly_returns,
)
from .formatting import metric_card, money, number, percent


def generate_html_report(result: BacktestResult, path: str | Path) -> Path:
    """Write a responsive single-file Plotly report."""

    performance = result.performance
    metrics = result.metrics
    figures = figure_html(
        [
            dashboard(performance),
            yearly_returns(performance),
            daily_distribution(performance),
            rolling_risk(performance, result.config.annualization_factor),
            benchmark_chart(result),
        ],
        result.config.plotly_js,
    )
    metric_specs = [
        (
            "累计收益率 (Total Return)",
            percent(metrics.get("total_return")),
            "positive" if metrics.get("total_return", 0) >= 0 else "negative",
        ),
        (
            "年化收益率 (CAGR)",
            percent(metrics.get("cagr")),
            "positive" if metrics.get("cagr", 0) >= 0 else "negative",
        ),
        ("夏普比率 (Sharpe)", number(metrics.get("sharpe")), ""),
        ("索提诺比率 (Sortino)", number(metrics.get("sortino")), ""),
        ("卡玛比率 (Calmar)", number(metrics.get("calmar")), ""),
        ("最大回撤 (Max DD)", percent(metrics.get("max_drawdown")), "negative"),
        ("波动率 (Volatility)", percent(metrics.get("volatility")), ""),
        ("胜率 (Win Rate)", percent(metrics.get("win_rate")), ""),
        ("盈亏比 (Profit Factor)", number(metrics.get("profit_factor")), ""),
        ("已完成交易数", str(metrics.get("closed_trades", 0)), ""),
        ("未平仓标的数", str(metrics.get("open_positions", 0)), ""),
        ("总交易成本", money(metrics.get("total_fees")), ""),
    ]
    cards = "".join(metric_card(*spec) for spec in metric_specs)
    benchmark_metrics = ""
    if result.config.benchmark:
        benchmark_metrics = (
            '<div class="analysis-grid">'
            + metric_card("基准", result.config.benchmark)
            + metric_card("累计超额收益", percent(metrics.get("total_excess_return")))
            + metric_card("跟踪误差", percent(metrics.get("tracking_error")))
            + metric_card("信息比率", number(metrics.get("information_ratio")))
            + metric_card("Beta", number(metrics.get("beta"), 4))
            + metric_card("Alpha（年化）", percent(metrics.get("alpha")))
            + "</div>"
        )

    fee_rows = "".join(
        [
            f"<tr><td>佣金</td><td>{money(metrics.get('total_commission'))}</td></tr>",
            f"<tr><td>印花税</td><td>{money(metrics.get('total_stamp_tax'))}</td></tr>",
            f"<tr><td>过户费</td><td>{money(metrics.get('total_transfer_fee'))}</td></tr>",
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
<div class="section-title">费用与交易限制 (Fees & Trading Constraints)</div><div class="row">
<div class="table-wrap"><table><thead><tr><th>费用项目</th><th>金额</th></tr></thead><tbody>{fee_rows}</tbody></table></div>
<div class="table-wrap"><table><thead><tr><th>拒单原因</th><th>次数</th></tr></thead><tbody>{rejection_rows(result)}</tbody></table></div>
</div>
<div class="section-title">组合归因 (Attribution)</div><div class="table-wrap"><table><thead><tr><th>标的</th><th>成交次数</th><th>持有总天数</th><th>已实现盈亏</th><th>盈亏贡献占比</th><th>每日权重贡献累计</th><th>总费用</th></tr></thead><tbody>{attribution_rows(result)}</tbody></table></div>
<footer>TuAlpha Report · Powered by Plotly · 股票与 ETF 日频回测</footer>
</div></body></html>"""
    destination = Path(path)
    destination.write_text(html, encoding="utf-8")
    return destination
