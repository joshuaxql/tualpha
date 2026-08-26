from __future__ import annotations

from pathlib import Path

import duckdb
from fakes import FakeProClient

from tualpha import local_data
from tualpha.bundle import latest_bundle_path, validate_parquet_bundle
from tualpha.data.bundle.parquet_store import CATALOG_FILE, load_manifest
from tualpha.data.quality import QualityReporter, QualityRunner
from tualpha.updater import DataUpdater, UpdateOptions


def _build(csv_dir: Path, root: Path) -> None:
    DataUpdater(
        UpdateOptions(
            bundle_root=root,
            start="20240102",
            end="20240108",
            full=True,
            retries=1,
            show_progress=False,
        ),
        client=FakeProClient(csv_dir),
    ).run()


def test_bundle_is_year_partitioned_parquet(bundle_root: Path) -> None:
    bundle = latest_bundle_path(bundle_root)
    manifest = validate_parquet_bundle(bundle, full_hash=True)

    assert manifest["protocol"] == "tualpha.parquet/1"
    assert manifest["partitioning"] == "year/v1"
    assert (bundle / "parquet/stock/daily/year=2024/data.parquet").is_file()
    assert (bundle / "parquet/etf/daily/year=2024/data.parquet").is_file()
    assert (bundle / "parquet/index/daily/year=2024/data.parquet").is_file()
    assert not list(bundle.rglob("*.h5"))
    assert not list(bundle.rglob("*.npy"))
    assert not list(bundle.rglob("*.pk"))


def test_catalog_generation_and_local_queries_match(bundle_root: Path) -> None:
    bundle = latest_bundle_path(bundle_root)
    manifest = load_manifest(bundle)
    connection = duckdb.connect(str(bundle / CATALOG_FILE), read_only=True)
    try:
        assert (
            connection.execute(
                "SELECT value FROM bundle_metadata WHERE key='generation'"
            ).fetchone()[0]
            == manifest["generation"]
        )
        assert (
            connection.execute("SELECT count(*) FROM assets WHERE tradable").fetchone()[
                0
            ]
            == 4
        )
    finally:
        connection.close()

    with local_data(bundle_root) as client:
        daily = client.query(
            "stock_daily",
            fields="ts_code,trade_date,close",
            filters={"ts_code": "000001.SZ"},
            start_date="20240102",
            end_date="20240104",
        )
        assert daily["close"].tolist() == [10.0, 11.0, 11.0]
        aggregate = client.sql(
            "SELECT trade_date, count(*) AS rows FROM stock_daily GROUP BY trade_date ORDER BY trade_date"
        )
        assert aggregate["rows"].tolist() == [2, 2, 2, 2, 2]
        assert client.sql("SELECT DISTINCT ts_code FROM index_daily ORDER BY ts_code")[
            "ts_code"
        ].tolist() == ["000985.CSI"]


def test_incremental_update_is_idempotent(csv_dir: Path, tmp_path: Path) -> None:
    root = tmp_path / "root"
    _build(csv_dir, root)
    options = UpdateOptions(
        bundle_root=root,
        start="20240108",
        end="20240108",
        lookback=0,
        retries=1,
        show_progress=False,
    )

    first = DataUpdater(options, client=FakeProClient(csv_dir)).run()
    second = DataUpdater(options, client=FakeProClient(csv_dir)).run()

    assert first.updated_dates == second.updated_dates == ("20240108",)
    with local_data(root) as client:
        duplicates = client.sql(
            "SELECT count(*) FROM ("
            "SELECT ts_code, trade_date, count(*) n FROM stock_daily "
            "GROUP BY ts_code, trade_date HAVING n > 1)"
        ).iloc[0, 0]
        assert duplicates == 0
        assert (
            client.sql(
                "SELECT count(*) FROM stock_daily WHERE trade_date='20240108'"
            ).iloc[0, 0]
            == 2
        )
        assert client.sql("SELECT DISTINCT ts_code FROM index_daily ORDER BY ts_code")[
            "ts_code"
        ].tolist() == ["000985.CSI"]


def test_quality_report_covers_local_tables(bundle_root: Path, tmp_path: Path) -> None:
    report = QualityRunner(bundle_root).run()
    output = QualityReporter(tmp_path / "quality").write(report)

    assert report.fail_count == 0, [
        (item.table, item.rule, item.count, item.message) for item in report.findings
    ]
    assert len(report.summaries) == 20
    rows = {summary.table: summary.rows for summary in report.summaries}
    assert all(
        0 < int(metric["value"]) < rows[str(metric["table"])]
        for metric in report.metrics
    )
    assert (output / "summary.csv").is_file()
    assert (output / "findings.csv").is_file()
    assert (output / "metrics.csv").is_file()
    assert (output / "report.json").is_file()
    assert (output / "report.html").is_file()
