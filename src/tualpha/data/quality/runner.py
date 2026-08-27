from __future__ import annotations

from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb

from ...foundation.config import DEFAULT_BUNDLE_ROOT
from ..bundle.manager import BUNDLE_NAME
from ..bundle.parquet_schema import (
    ADJ_FACTOR,
    ETF_ADJ_FACTOR,
    ETF_DAILY,
    FINANCE_SPECS,
    INDEX_DAILY,
    INDEX_WEIGHT,
    STOCK_DAILY,
    TABLE_SPECS,
    TableSpec,
)
from ..bundle.parquet_store import validate_bundle
from ..query import LocalDataClient
from .models import QualityFinding, QualityReport, Severity, TableSummary


def _ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


class QualityRunner:
    """Run structural, key, coverage, domain, and cross-table checks in DuckDB."""

    def __init__(
        self,
        bundle_root: str | Path = DEFAULT_BUNDLE_ROOT,
        bundle_name: str = BUNDLE_NAME,
    ) -> None:
        self.bundle_root = Path(bundle_root).expanduser()
        self.bundle_name = bundle_name

    def run(
        self,
        tables: tuple[str, ...] | None = None,
        *,
        full_hash: bool = False,
    ) -> QualityReport:
        names = tables or tuple(TABLE_SPECS)
        unknown = set(names).difference(TABLE_SPECS)
        if unknown:
            raise ValueError(f"unknown quality tables: {sorted(unknown)}")
        with LocalDataClient(self.bundle_root, self.bundle_name) as client:
            manifest = validate_bundle(client.bundle_path, full_hash=full_hash)
            findings: list[QualityFinding] = []
            summaries: list[TableSummary] = []
            metrics: list[dict[str, object]] = []
            dataset_rows = {
                str(row[0]): (int(row[1]), int(row[2]))
                for row in client.connection.execute(
                    "SELECT table_name, row_count, partition_count FROM catalog.datasets"
                ).fetchall()
            }
            for name in names:
                spec = TABLE_SPECS[name]
                table_findings, table_metrics, bounds = self._check_table(client, spec)
                findings.extend(table_findings)
                metrics.extend(table_metrics)
                rows, partitions = dataset_rows.get(name, (0, 0))
                summaries.append(
                    TableSummary(
                        table=name,
                        rows=rows,
                        partitions=partitions,
                        start_date=bounds[0],
                        end_date=bounds[1],
                        fail_count=sum(
                            1
                            for item in table_findings
                            if item.severity is Severity.FAIL
                        ),
                        warn_count=sum(
                            1
                            for item in table_findings
                            if item.severity is Severity.WARN
                        ),
                    )
                )
            findings.extend(self._cross_table_checks(client, set(names)))
            # Cross-table findings must be reflected in their table summaries.
            for index, summary in enumerate(summaries):
                cross = [item for item in findings if item.table == summary.table]
                summaries[index] = TableSummary(
                    table=summary.table,
                    rows=summary.rows,
                    partitions=summary.partitions,
                    start_date=summary.start_date,
                    end_date=summary.end_date,
                    fail_count=sum(item.severity is Severity.FAIL for item in cross),
                    warn_count=sum(item.severity is Severity.WARN for item in cross),
                )
        return QualityReport(
            generation=str(manifest["generation"]),
            created_at=datetime.now(ZoneInfo("UTC")).isoformat(),
            summaries=summaries,
            findings=findings,
            metrics=metrics,
        )

    def _check_table(
        self, client: LocalDataClient, spec: TableSpec
    ) -> tuple[
        list[QualityFinding], list[dict[str, object]], tuple[str | None, str | None]
    ]:
        findings: list[QualityFinding] = []
        metrics: list[dict[str, object]] = []
        table = _ident(spec.name)
        try:
            actual_columns = {
                str(row[0])
                for row in client.connection.execute(f"DESCRIBE {table}").fetchall()
            }
        except duckdb.Error as exc:
            return (
                [
                    QualityFinding(
                        spec.name,
                        Severity.FAIL,
                        "readable_table",
                        1,
                        f"table cannot be scanned: {exc}",
                    )
                ],
                metrics,
                (None, None),
            )
        missing = set(spec.column_names).difference(actual_columns)
        if missing:
            findings.append(
                QualityFinding(
                    spec.name,
                    Severity.FAIL,
                    "required_columns",
                    len(missing),
                    f"missing required columns: {sorted(missing)}",
                )
            )
            return findings, metrics, (None, None)

        bounds: tuple[str | None, str | None] = (None, None)
        if spec.date_column:
            row = client.connection.execute(
                f"SELECT min({_ident(spec.date_column)}), max({_ident(spec.date_column)}) FROM {table}"
            ).fetchone()
            bounds = (
                None if row is None or row[0] is None else str(row[0]),
                None if row is None or row[1] is None else str(row[1]),
            )

        required_key = (
            ("ts_code", "ann_date", "end_date")
            if spec.name in FINANCE_SPECS
            else spec.primary_key
        )
        pk_condition = " OR ".join(
            f"{_ident(column)} IS NULL OR trim(CAST({_ident(column)} AS VARCHAR)) = ''"
            for column in required_key
        )
        self._finding_query(
            client,
            findings,
            spec.name,
            Severity.FAIL,
            "null_primary_key",
            f"SELECT * FROM {table} WHERE {pk_condition}",
            "primary key contains null or empty values",
        )
        group = ", ".join(_ident(column) for column in spec.primary_key)
        self._finding_query(
            client,
            findings,
            spec.name,
            Severity.FAIL,
            "duplicate_primary_key",
            f"SELECT {group}, count(*) AS duplicates FROM {table} GROUP BY {group} HAVING count(*) > 1",
            "duplicate primary keys",
            count_expression="coalesce(sum(duplicates - 1), 0)",
        )
        if spec.date_column:
            date = _ident(spec.date_column)
            self._finding_query(
                client,
                findings,
                spec.name,
                Severity.FAIL,
                "valid_date",
                f"SELECT * FROM {table} WHERE try_strptime(CAST({date} AS VARCHAR), '%Y%m%d') IS NULL",
                f"{spec.date_column} contains invalid YYYYMMDD values",
            )
            if spec.partition_column in {"year", "report_year"}:
                partition = "year" if spec.partition_column == "year" else "report_year"
                self._finding_query(
                    client,
                    findings,
                    spec.name,
                    Severity.FAIL,
                    "partition_matches_date",
                    f"SELECT * FROM {table} WHERE CAST({_ident(partition)} AS VARCHAR) <> substr(CAST({date} AS VARCHAR), 1, 4)",
                    "Hive year partition does not match row date",
                )

        # Report only partial gaps; zero-null and entirely unavailable columns are noise.
        expressions = [
            f"sum(CASE WHEN {_ident(column)} IS NULL THEN 1 ELSE 0 END) AS {_ident(column)}"
            for column in spec.column_names
        ]
        nulls = client.connection.execute(
            f"SELECT count(*) AS row_count, {', '.join(expressions)} FROM {table}"
        ).fetchone()
        if nulls is not None:
            row_count = int(nulls[0])
            metrics.extend(
                {
                    "table": spec.name,
                    "metric": "null_count",
                    "column": column,
                    "value": int(value),
                }
                for column, value in zip(spec.column_names, nulls[1:], strict=True)
                if value is not None and 0 < int(value) < row_count
            )

        self._domain_checks(client, spec, findings)
        return findings, metrics, bounds

    def _finding_query(
        self,
        client: LocalDataClient,
        findings: list[QualityFinding],
        table: str,
        severity: Severity,
        rule: str,
        query: str,
        message: str,
        *,
        count_expression: str = "count(*)",
    ) -> None:
        count = int(
            client.connection.execute(
                f"SELECT {count_expression} FROM ({query}) findings"
            ).fetchone()[0]
        )
        if not count:
            return
        sample = client.connection.execute(
            f"SELECT * FROM ({query}) findings LIMIT 5"
        ).fetchdf()
        findings.append(
            QualityFinding(
                table=table,
                severity=severity,
                rule=rule,
                count=count,
                message=message,
                sample=sample.to_dict("records"),
            )
        )

    def _domain_checks(
        self,
        client: LocalDataClient,
        spec: TableSpec,
        findings: list[QualityFinding],
    ) -> None:
        table = _ident(spec.name)
        if spec in {STOCK_DAILY, ETF_DAILY, INDEX_DAILY}:
            self._finding_query(
                client,
                findings,
                spec.name,
                Severity.WARN,
                "ohlc_domain",
                f"SELECT ts_code, trade_date, open, high, low, close FROM {table} "
                "WHERE open <= 0 OR high <= 0 OR low <= 0 OR close <= 0 "
                "OR high < greatest(open, close) OR low > least(open, close) OR high < low",
                "OHLC values or relationships are invalid",
            )
            self._finding_query(
                client,
                findings,
                spec.name,
                Severity.FAIL,
                "nonnegative_activity",
                f"SELECT ts_code, trade_date, volume, turnover FROM {table} WHERE volume < 0 OR turnover < 0",
                "volume or turnover is negative",
            )
        elif spec in {ADJ_FACTOR, ETF_ADJ_FACTOR}:
            self._finding_query(
                client,
                findings,
                spec.name,
                Severity.FAIL,
                "positive_adjustment_factor",
                f"SELECT * FROM {table} WHERE adj_factor IS NULL OR adj_factor <= 0",
                "adjustment factors must be finite and positive",
            )
        elif spec.name == "stk_limit":
            self._finding_query(
                client,
                findings,
                spec.name,
                Severity.WARN,
                "limit_domain",
                f"SELECT * FROM {table} WHERE up_limit <= 0 OR down_limit <= 0 OR down_limit > up_limit",
                "daily price limits are invalid",
            )
        elif spec is INDEX_WEIGHT:
            self._finding_query(
                client,
                findings,
                spec.name,
                Severity.FAIL,
                "weight_domain",
                f"SELECT * FROM {table} WHERE weight IS NULL OR weight < 0 OR weight > 100",
                "index constituent weight is outside [0, 100]",
            )
            self._finding_query(
                client,
                findings,
                spec.name,
                Severity.WARN,
                "weight_sum",
                f"SELECT index_code, trade_date, sum(weight) AS total_weight FROM {table} "
                "GROUP BY index_code, trade_date HAVING total_weight < 95 OR total_weight > 105",
                "index snapshot weights do not sum to approximately 100",
            )
        elif spec.name in FINANCE_SPECS:
            self._finding_query(
                client,
                findings,
                spec.name,
                Severity.WARN,
                "finance_pit_dates",
                f"SELECT ts_code, ann_date, f_ann_date, effective_ann_date, end_date FROM {table} "
                "WHERE effective_ann_date IS NULL OR end_date IS NULL "
                "OR effective_ann_date < end_date",
                "financial PIT announcement/report dates are invalid",
            )

    def _cross_table_checks(
        self, client: LocalDataClient, selected: set[str]
    ) -> list[QualityFinding]:
        findings: list[QualityFinding] = []
        checks = [
            (
                "stock_daily",
                "stock_basic",
                "stock daily codes missing from stock_basic",
            ),
            ("etf_daily", "etf_basic", "ETF daily codes missing from etf_basic"),
            (
                "index_daily",
                "index_basic",
                "index daily codes missing from index_basic",
            ),
        ]
        for table, master, message in checks:
            if table not in selected:
                continue
            self._finding_query(
                client,
                findings,
                table,
                Severity.WARN,
                "master_reference",
                f"SELECT DISTINCT d.ts_code FROM {_ident(table)} d "
                f"LEFT JOIN {_ident(master)} m USING(ts_code) WHERE m.ts_code IS NULL",
                message,
            )
        if "adj_factor" in selected:
            self._finding_query(
                client,
                findings,
                "adj_factor",
                Severity.WARN,
                "daily_coverage",
                "SELECT d.ts_code, d.trade_date FROM stock_daily d LEFT JOIN adj_factor a USING(ts_code, trade_date) "
                "WHERE a.ts_code IS NULL",
                "stock daily rows without an adjustment factor",
            )
        if "etf_adj_factor" in selected:
            self._finding_query(
                client,
                findings,
                "etf_adj_factor",
                Severity.WARN,
                "daily_coverage",
                "SELECT d.ts_code, d.trade_date FROM etf_daily d LEFT JOIN etf_adj_factor a USING(ts_code, trade_date) "
                "WHERE a.ts_code IS NULL",
                "ETF daily rows without an adjustment factor",
            )
        return findings
