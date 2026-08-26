from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
import pytest
from fakes import FakeProClient

from tualpha import local_data
from tualpha.bundle import bundle_path, load_bundle_data, update_status_path
from tualpha.cli import run_cli
from tualpha.data.bundle.parquet_store import load_manifest
from tualpha.exceptions import ConfigurationError, DataError
from tualpha.updater import (
    DEFAULT_INDEX_WEIGHT_CODES,
    DataUpdater,
    UpdateOptions,
    UpdateResult,
)


def _options(root: Path, **kwargs: Any) -> UpdateOptions:
    return UpdateOptions(
        bundle_root=root,
        start="20240108",
        end="20240108",
        lookback=0,
        retries=1,
        show_progress=False,
        **kwargs,
    )


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


def test_update_cli_rejects_removed_csv_directory_option(tmp_path: Path) -> None:
    with pytest.raises(SystemExit) as error:
        run_cli(["update", "--csv-dir", str(tmp_path)])
    assert error.value.code == 2


def test_compact_cli_does_not_require_tushare_token(
    bundle_root: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.delenv("TUSHARE_TOKEN", raising=False)
    assert run_cli(["compact", "--bundle-root", str(bundle_root), "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["bundle_path"] == str(bundle_root / "bundle")


def test_update_rejects_invalid_options(tmp_path: Path) -> None:
    with pytest.raises(ConfigurationError, match="bundle_name"):
        UpdateOptions(bundle_root=tmp_path, bundle_name="../escape")
    with pytest.raises(ConfigurationError, match="lookback"):
        UpdateOptions(bundle_root=tmp_path, lookback=-1)


def test_update_cli_shows_progress_by_default(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    received: list[UpdateOptions] = []

    class StubUpdater:
        def __init__(self, options: UpdateOptions, *, token: str) -> None:
            assert token == "test-token"
            received.append(options)

        @staticmethod
        def run() -> UpdateResult:
            return UpdateResult("test", (), 0, None, True)

    monkeypatch.setenv("TUSHARE_TOKEN", "test-token")
    monkeypatch.setattr("tualpha.cmds.entry.DataUpdater", StubUpdater)
    assert run_cli(["update", "--bundle-root", str(tmp_path)]) == 0
    capsys.readouterr()
    assert received[-1].show_progress is True
    assert run_cli(["update", "--bundle-root", str(tmp_path), "--no-progress"]) == 0
    capsys.readouterr()
    assert received[-1].show_progress is False
    assert run_cli(["update", "--bundle-root", str(tmp_path), "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["run_id"] == "test"
    assert received[-1].show_progress is False


def test_update_requires_token_without_injected_client(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("TUSHARE_TOKEN", raising=False)
    with pytest.raises(ConfigurationError, match="TUSHARE_TOKEN"):
        DataUpdater(UpdateOptions(bundle_root=tmp_path))
    assert run_cli(["update", "--bundle-root", str(tmp_path)]) == 2


def test_incremental_index_weight_refresh_does_not_redownload_full_history(
    csv_dir: Path, tmp_path: Path
) -> None:
    updater = DataUpdater(_options(tmp_path / "root"), client=FakeProClient(csv_dir))
    active = pd.DatetimeIndex(["2010-01-04", "2026-08-21"])

    assert (
        updater._index_weight_history_start(
            ["20260812", "20260821"], "20260821", active
        )
        == "20260701"
    )


def test_incremental_financial_update_uses_announcement_lookback(
    csv_dir: Path, tmp_path: Path
) -> None:
    client = FakeProClient(csv_dir)
    updater = DataUpdater(_options(tmp_path / "root"), client=client)
    from tualpha.data.bundle.csv_cache import CsvUpdateWriter

    with CsvUpdateWriter(tmp_path / "cache", []) as writer:
        updater._download_financials(writer, "20240415", None)

    calls = [params for name, params in client.calls if name.endswith("_vip")]
    assert len(calls) == 4
    assert {params["start_date"] for params in calls} == {"20231217"}
    assert {params["end_date"] for params in calls} == {"20240415"}
    assert all("period" not in params for params in calls)


def test_initial_online_update_builds_parquet_bundle(
    csv_dir: Path, tmp_path: Path
) -> None:
    root = tmp_path / "root"
    source_before = {
        path.relative_to(csv_dir): path.read_bytes()
        for path in csv_dir.rglob("*")
        if path.is_file()
    }
    client = FakeProClient(csv_dir)

    result = DataUpdater(_options(root), client=client).run()

    assert result.bundle_path == str(bundle_path(root))
    assert {path.name for path in bundle_path(root).iterdir()} == {
        "catalog.duckdb",
        "manifest.json",
        "parquet",
    }
    assert not list(root.rglob("*.h5"))
    assert not list(root.rglob("*.csv"))
    assert source_before == {
        path.relative_to(csv_dir): path.read_bytes()
        for path in csv_dir.rglob("*")
        if path.is_file()
    }
    manifest = load_manifest(bundle_path(root))
    assert (
        manifest["build_pipeline"]
        == "tushare->partitioned-csv->year-partitioned-parquet"
    )
    assert any(name == "index_basic" for name, _ in client.calls)
    assert {
        params["index_code"] for name, params in client.calls if name == "index_weight"
    } == set(DEFAULT_INDEX_WEIGHT_CODES)
    status = json.loads(update_status_path(root).read_text(encoding="utf-8"))
    assert status["storage"] == "parquet+duckdb"


def test_update_accepts_missing_master_dates(csv_dir: Path, tmp_path: Path) -> None:
    class MissingDates(FakeProClient):
        def query(self, api_name: str, **params: Any) -> pd.DataFrame:
            frame = super().query(api_name, **params)
            if api_name == "stock_basic":
                return frame.drop(columns=["delist_date"])
            if api_name == "etf_basic" and not frame.empty:
                frame = frame.copy()
                frame.loc[frame.index[0], "list_date"] = None
            return frame

    root = tmp_path / "root"
    DataUpdater(_options(root), client=MissingDates(csv_dir)).run()
    with load_bundle_data(root) as loaded:
        assert loaded.asset_finder.retrieve_asset("510300.SH").name == "300ETF"


def test_incremental_update_is_authoritative_and_idempotent(
    csv_dir: Path, tmp_path: Path
) -> None:
    root = tmp_path / "root"
    _build(csv_dir, root)
    before = load_manifest(root / "bundle")["generation"]

    DataUpdater(_options(root), client=FakeProClient(csv_dir)).run()
    DataUpdater(_options(root), client=FakeProClient(csv_dir)).run()

    after = load_manifest(root / "bundle")["generation"]
    assert after != before
    with local_data(root) as client:
        assert (
            client.sql(
                "SELECT count(*) FROM stock_daily WHERE trade_date='20240108'"
            ).iloc[0, 0]
            == 2
        )
        assert (
            client.sql(
                "SELECT count(*) FROM (SELECT ts_code, trade_date, count(*) n "
                "FROM stock_daily GROUP BY ts_code, trade_date HAVING n > 1)"
            ).iloc[0, 0]
            == 0
        )


def test_repair_clears_removed_st_and_resumption_is_not_suspended(
    csv_dir: Path, tmp_path: Path
) -> None:
    class RemovedFlags(FakeProClient):
        def query(self, api_name: str, **params: Any) -> pd.DataFrame:
            frame = super().query(api_name, **params)
            if api_name == "stock_st" and params["trade_date"] == "20240104":
                return frame.iloc[0:0]
            if api_name == "suspend_d" and params["trade_date"] == "20240105":
                frame = frame.copy()
                frame["suspend_type"] = "R"
            return frame

    root = tmp_path / "root"
    _build(csv_dir, root)
    DataUpdater(
        UpdateOptions(
            bundle_root=root,
            start="20240104",
            end="20240105",
            lookback=0,
            retries=1,
            show_progress=False,
        ),
        client=RemovedFlags(csv_dir),
    ).run()

    with local_data(root) as client:
        assert (
            client.sql(
                "SELECT count(*) FROM stock_st WHERE trade_date='20240104'"
            ).iloc[0, 0]
            == 0
        )
        assert (
            client.sql(
                "SELECT count(*) FROM suspend_d WHERE trade_date='20240105'"
            ).iloc[0, 0]
            == 0
        )


def test_index_weight_refresh_keeps_other_indices(
    csv_dir: Path, tmp_path: Path
) -> None:
    root = tmp_path / "root"
    _build(csv_dir, root)
    with local_data(root) as query:
        before = {
            code: count
            for code, count in query.connection.execute(
                "SELECT index_code, count(*) FROM index_weight GROUP BY index_code"
            ).fetchall()
        }

    DataUpdater(_options(root), client=FakeProClient(csv_dir)).run()

    with local_data(root) as query:
        after = {
            code: count
            for code, count in query.connection.execute(
                "SELECT index_code, count(*) FROM index_weight GROUP BY index_code"
            ).fetchall()
        }
    assert set(after) == set(before) == set(DEFAULT_INDEX_WEIGHT_CODES)


def test_failed_build_keeps_active_generation_and_source(
    csv_dir: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "root"
    _build(csv_dir, root)
    generation = load_manifest(root / "bundle")["generation"]

    def fail(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("simulated build failure")

    monkeypatch.setattr("tualpha.data.bundle.updater.build_parquet_bundle", fail)
    with pytest.raises(DataError, match="simulated build failure"):
        DataUpdater(_options(root), client=FakeProClient(csv_dir)).run()

    assert load_manifest(root / "bundle")["generation"] == generation
    status = json.loads(update_status_path(root).read_text(encoding="utf-8"))
    assert status["status"] == "failed"
    assert Path(status["staging_path"]).is_dir()


def test_first_full_update_requires_explicit_start(
    csv_dir: Path, tmp_path: Path
) -> None:
    with pytest.raises(DataError, match="explicit --from"):
        DataUpdater(
            UpdateOptions(bundle_root=tmp_path / "root", full=True, retries=1),
            client=FakeProClient(csv_dir),
        ).run()


def test_build_cli_uses_full_update_pipeline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    received: list[UpdateOptions] = []

    class StubUpdater:
        def __init__(self, options: UpdateOptions, *, token: str) -> None:
            assert token == "test-token"
            received.append(options)

        @staticmethod
        def run() -> UpdateResult:
            return UpdateResult("build", ("20240102",), 3, "bundle", False)

    monkeypatch.setenv("TUSHARE_TOKEN", "test-token")
    monkeypatch.setattr("tualpha.cmds.entry.DataUpdater", StubUpdater)
    assert (
        run_cli(
            [
                "build",
                "--from",
                "20240102",
                "--to",
                "20240108",
                "--bundle-root",
                str(tmp_path),
                "--json",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["operation"] == "build"
    assert payload["updated_dates"] == ["20240102"]
    assert received[-1].full is True
    assert received[-1].start == "20240102"
    assert received[-1].end == "20240108"
    assert received[-1].lookback == 0
    assert received[-1].show_progress is False

    with pytest.raises(SystemExit) as removed:
        run_cli(["import-csv"])
    assert removed.value.code == 2
    with pytest.raises(SystemExit) as old_full:
        run_cli(["update", "--full"])
    assert old_full.value.code == 2


def test_quality_cli_after_full_build(
    csv_dir: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    root = tmp_path / "root"
    _build(csv_dir, root)
    assert run_cli(["quality", "--bundle-root", str(root), "--json"]) == 0
    quality = json.loads(capsys.readouterr().out)
    assert quality["fail"] == 0
