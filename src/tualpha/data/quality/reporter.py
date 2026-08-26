from __future__ import annotations

import html
import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from .models import QualityReport


class QualityReporter:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root).expanduser()

    def write(self, report: QualityReport) -> Path:
        run_id = datetime.now(ZoneInfo("UTC")).strftime("%Y%m%d_%H%M%S_%f")
        destination = self.root / run_id
        destination.mkdir(parents=True, exist_ok=False)
        summaries = pd.DataFrame([item.to_dict() for item in report.summaries])
        findings = pd.DataFrame(
            [
                {
                    **item.to_dict(),
                    "severity": item.severity.value,
                    "sample": json.dumps(item.sample, ensure_ascii=False, default=str),
                }
                for item in report.findings
            ],
            columns=["table", "severity", "rule", "count", "message", "sample"],
        )
        metrics = pd.DataFrame(report.metrics)
        summaries.to_csv(destination / "summary.csv", index=False, encoding="utf-8-sig")
        findings.to_csv(destination / "findings.csv", index=False, encoding="utf-8-sig")
        metrics.to_csv(destination / "metrics.csv", index=False, encoding="utf-8-sig")
        payload = {
            "generation": report.generation,
            "created_at": report.created_at,
            "fail_count": report.fail_count,
            "warn_count": report.warn_count,
            "summaries": [item.to_dict() for item in report.summaries],
            "findings": [
                {**item.to_dict(), "severity": item.severity.value}
                for item in report.findings
            ],
            "metrics": report.metrics,
        }
        (destination / "report.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
        (destination / "report.html").write_text(
            self._html(report, summaries, findings, metrics), encoding="utf-8"
        )
        report.output_dir = destination
        return destination

    @staticmethod
    def _html(
        report: QualityReport,
        summaries: pd.DataFrame,
        findings: pd.DataFrame,
        metrics: pd.DataFrame,
    ) -> str:
        status = (
            "失败" if report.fail_count else "有警告" if report.warn_count else "通过"
        )
        findings_html = (
            findings.to_html(index=False, escape=True)
            if not findings.empty
            else "<p>未发现问题。</p>"
        )
        metrics_html = (
            metrics.to_html(index=False, escape=True)
            if not metrics.empty
            else "<p>无指标。</p>"
        )
        return f"""<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8"><title>TuAlpha 数据质量报告</title>
<style>
body{{font-family:system-ui,"Microsoft YaHei",sans-serif;margin:32px;color:#17202a;background:#f7f9fb}}
h1,h2{{color:#123b5d}} .cards{{display:flex;gap:16px;margin:20px 0}} .card{{background:white;padding:16px 22px;border-radius:10px;box-shadow:0 2px 8px #0001}}
table{{border-collapse:collapse;width:100%;background:white;font-size:13px;margin-bottom:28px}}th,td{{border:1px solid #dbe3ea;padding:7px;text-align:left;vertical-align:top}}th{{background:#eaf1f7}}tr:nth-child(even){{background:#f8fbfd}}
.fail{{color:#b42318}} .warn{{color:#b54708}} code{{word-break:break-all}}
</style></head><body>
<h1>TuAlpha 本地数据质量报告</h1>
<div class="cards"><div class="card"><b>状态</b><br>{html.escape(status)}</div><div class="card"><b>失败</b><br><span class="fail">{report.fail_count}</span></div><div class="card"><b>警告</b><br><span class="warn">{report.warn_count}</span></div><div class="card"><b>Generation</b><br><code>{html.escape(report.generation)}</code></div></div>
<p>生成时间：{html.escape(report.created_at)}</p>
<h2>表级摘要</h2>{summaries.to_html(index=False, escape=True)}
<h2>问题明细</h2>{findings_html}
<h2>列级指标</h2>{metrics_html}
</body></html>"""


def format_summary(report: QualityReport) -> str:
    lines = [
        (
            f"quality generation={report.generation} tables={len(report.summaries)} "
            f"fail={report.fail_count} warn={report.warn_count}"
        )
    ]
    lines.extend(
        f"{item.table}: {item.status} rows={item.rows} partitions={item.partitions} "
        f"fail={item.fail_count} warn={item.warn_count}"
        for item in report.summaries
    )
    if report.output_dir is not None:
        lines.append(f"report: {report.output_dir}")
    return "\n".join(lines)
