from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import pytest

from tualpha.bundle import (
    NormalizedStore,
    bundle_path,
    normalized_store_path,
    update_status_path,
)
from tualpha.cli import run_cli
from tualpha.exceptions import ConfigurationError, DataError
from tualpha.updater import DataUpdater, UpdateOptions


class FakeProClient:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def _daily(self, api_name: str, date: str) -> pd.DataFrame:
        path = self.root / api_name / f"{date}.csv"
        return pd.read_csv(path, dtype={"ts_code": str})

    def query(self, api_name: str, **params: Any) -> pd.DataFrame:
        self.calls.append((api_name, dict(params)))
        offset = int(params.get("offset", 0))
        limit = int(params.get("limit", 5000))
        if api_name == "stock_basic":
            frame = pd.read_csv(self.root / "stock_basic.csv", dtype=str)
            frame = frame[frame["list_status"] == params.get("list_status")]
        elif api_name == "etf_basic":
            frame = pd.read_csv(self.root / "etf_basic.csv", dtype=str)
            frame = frame[frame["list_status"] == params.get("list_status")]
        elif api_name == "index_basic":
            frame = pd.read_csv(self.root / "index_basic.csv", dtype=str)
            frame = frame[frame["market"] == params.get("market")]
        elif api_name == "trade_cal":
            frame = pd.read_csv(self.root / "trade_cal.csv", dtype=str)
            shenzhen = frame.copy()
            shenzhen["exchange"] = "SZSE"
            frame = pd.concat([frame, shenzhen], ignore_index=True)
        elif api_name in {
            "daily",
            "adj_factor",
            "fund_daily",
            "fund_adj",
            "daily_basic",
            "moneyflow",
            "stk_limit",
            "suspend_d",
            "stock_st",
            "index_daily",
        }:
            frame = self._daily(api_name, str(params["trade_date"]))
        elif api_name == "index_member_all":
            source = pd.read_csv(self.root / "industry" / "20240108.csv", dtype=str)
            frame = source.drop(columns=["trade_date"]).copy()
            frame["name"] = "测试银行"
            frame["in_date"] = "20000101"
            frame["out_date"] = ""
            frame["is_new"] = "Y"
        elif api_name.endswith("_vip"):
            directory = api_name.removesuffix("_vip")
            if "period" in params:
                path = self.root / directory / f"{params['period']}.csv"
                frame = (
                    pd.read_csv(path, dtype=str) if path.is_file() else pd.DataFrame()
                )
            else:
                files = sorted((self.root / directory).glob("*.csv"))
                frame = pd.concat(
                    [pd.read_csv(path, dtype=str) for path in files],
                    ignore_index=True,
                    sort=False,
                )
                effective = frame.get("f_ann_date", frame["ann_date"]).fillna(
                    frame["ann_date"]
                )
                frame = frame[
                    (effective >= params["start_date"])
                    & (effective <= params["end_date"])
                ]
        elif api_name == "index_weight":
            frame = pd.DataFrame()
        else:  # pragma: no cover - catches unexpected updater API additions
            raise AssertionError(f"unexpected API: {api_name}")
        return frame.iloc[offset : offset + limit].reset_index(drop=True)


def test_update_requires_explicit_csv_directory() -> None:
    with pytest.raises(SystemExit) as error:
        run_cli(["update"])
    assert error.value.code == 2


def test_update_rejects_overlapping_csv_and_bundle_paths(tmp_path: Path) -> None:
    with pytest.raises(ConfigurationError, match="must not overlap"):
        UpdateOptions(csv_dir=tmp_path, bundle_root=tmp_path / ".tualpha")
    with pytest.raises(ConfigurationError, match="bundle_name"):
        UpdateOptions(
            csv_dir=tmp_path / "csv",
            bundle_root=tmp_path / "bundle",
            bundle_name="../escape",
        )


def test_update_requires_token_without_injected_client(
    csv_dir: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("TUSHARE_TOKEN", raising=False)
    with pytest.raises(ConfigurationError, match="TUSHARE_TOKEN"):
        DataUpdater(UpdateOptions(csv_dir=csv_dir, bundle_root=tmp_path))
    assert run_cli(["update", "--csv-dir", str(csv_dir)]) == 2


def test_incremental_update_replaces_current_bundle(
    csv_dir: Path, tmp_path: Path
) -> None:
    bundle_root = tmp_path / "bundle"
    bundle_root.mkdir()
    update_status_path(bundle_root).write_text(
        json.dumps({"last_bundle_build": {"bundle_path": "previous"}}),
        encoding="utf-8",
    )
    client = FakeProClient(csv_dir)
    options = UpdateOptions(
        csv_dir=csv_dir,
        bundle_root=bundle_root,
        start="20240108",
        end="20240108",
        lookback=0,
        retries=1,
    )
    result = DataUpdater(options, client=client).run()

    assert result.updated_dates == ("20240108",)
    assert result.bundle_path is not None
    status = json.loads(update_status_path(bundle_root).read_text(encoding="utf-8"))
    assert status["status"] == "succeeded"
    assert status["csv_dir"] == str(csv_dir.resolve())
    assert status["last_success"]["run_id"] == result.run_id
    assert status["last_bundle_build"]["bundle_path"] == "previous"
    assert status["requested_start"] == "20240108"
    assert status["requested_end"] == "20240108"
    assert status["lookback"] == 0
    assert any(name == "stk_limit" for name, _ in client.calls)
    assert any(name == "balancesheet_vip" for name, _ in client.calls)
    assert any(
        name == "balancesheet_vip" and "start_date" in params
        for name, params in client.calls
    )
    assert any(
        name == "fina_indicator_vip" and "invturn_days" in params.get("fields", "")
        for name, params in client.calls
    )
    assert not any(
        name == "fina_indicator_vip" and "start_date" in params
        for name, params in client.calls
    )
    assert len(pd.read_csv(csv_dir / "income" / "20230930.csv")) == 2
    assert result.bundle_path == str(bundle_path(bundle_root))


def test_dry_run_status_keeps_request_and_build_history(
    csv_dir: Path, tmp_path: Path
) -> None:
    bundle_root = tmp_path / "bundle"
    bundle_root.mkdir()
    update_status_path(bundle_root).write_text(
        json.dumps({"last_bundle_build": {"bundle_path": "previous-build"}}),
        encoding="utf-8",
    )
    result = DataUpdater(
        UpdateOptions(
            csv_dir=csv_dir,
            bundle_root=bundle_root,
            start="20240108",
            end="20240108",
            lookback=0,
            retries=1,
            dry_run=True,
        ),
        client=FakeProClient(csv_dir),
    ).run()

    assert result.dry_run is True
    status = json.loads(update_status_path(bundle_root).read_text(encoding="utf-8"))
    assert status["status"] == "dry_run_succeeded"
    assert status["requested_start"] == "20240108"
    assert status["requested_end"] == "20240108"
    assert status["last_bundle_build"]["bundle_path"] == "previous-build"


def test_failed_bundle_build_restores_published_csv_and_cache(
    csv_dir: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bundle_root = tmp_path / "bundle"
    NormalizedStore(csv_dir, bundle_root).rebuild()
    update_status_path(bundle_root).write_text(
        json.dumps(
            {
                "last_success": {"bundle_path": "previous"},
                "last_bundle_build": {"bundle_path": "previous-build"},
            }
        ),
        encoding="utf-8",
    )
    original = (csv_dir / "daily" / "20240108.csv").read_bytes()
    client = FakeProClient(csv_dir)
    original_query = client.query

    def changed_query(api_name: str, **params: Any) -> pd.DataFrame:
        frame = original_query(api_name, **params)
        if api_name == "daily" and not frame.empty:
            frame = frame.copy()
            frame.loc[:, "close"] = 999.0
        return frame

    client.query = changed_query  # type: ignore[method-assign]

    def fail_build(*args: Any, **kwargs: Any) -> None:
        raise DataError("simulated bundle failure")

    monkeypatch.setattr("tualpha.updater.build_bundle", fail_build)
    updater = DataUpdater(
        UpdateOptions(
            csv_dir=csv_dir,
            bundle_root=bundle_root,
            start="20240108",
            end="20240108",
            lookback=0,
            retries=1,
        ),
        client=client,
    )
    with pytest.raises(DataError, match="staging retained"):
        updater.run()
    assert (csv_dir / "daily" / "20240108.csv").read_bytes() == original
    status = json.loads(update_status_path(bundle_root).read_text(encoding="utf-8"))
    assert status["status"] == "failed"
    assert status["last_success"]["bundle_path"] == "previous"
    assert status["last_bundle_build"]["bundle_path"] == "previous-build"
    assert status["requested_start"] == "20240108"
    assert status["requested_end"] == "20240108"
    connection = duckdb.connect(str(normalized_store_path(bundle_root)), read_only=True)
    try:
        close = connection.execute(
            "SELECT close FROM prices WHERE ts_code = '000001.SZ' "
            "AND trade_date = DATE '2024-01-08'"
        ).fetchone()[0]
    finally:
        connection.close()
    assert close == 11.0


def test_interrupted_csv_publication_is_recovered(
    tmp_path: Path,
) -> None:
    csv_root = tmp_path / "csv"
    bundle_root = tmp_path / "bundle"
    destination = csv_root / "daily" / "20240108.csv"
    destination.parent.mkdir(parents=True)
    destination.write_text("old\n", encoding="utf-8")
    staging = bundle_root / ".staging" / "update" / "interrupted"
    staged = staging / "daily" / "20240108.csv"
    staged.parent.mkdir(parents=True)
    staged.write_text("new\n", encoding="utf-8")
    DataUpdater._publish_files([(staged, destination)], staging / "backups")
    cache = normalized_store_path(bundle_root)
    cache.parent.mkdir(parents=True)
    cache.write_bytes(b"stale cache")
    update_status_path(bundle_root).write_text(
        json.dumps(
            {
                "status": "running",
                "run_id": "interrupted",
                "started_at": "2024-01-01T00:00:00+00:00",
                "csv_dir": str(csv_root.resolve()),
                "bundle_root": str(bundle_root.resolve()),
                "bundle_name": "tualpha",
                "staging_path": str(staging),
            }
        ),
        encoding="utf-8",
    )
    updater = DataUpdater(
        UpdateOptions(csv_dir=csv_root, bundle_root=bundle_root),
        client=FakeProClient(csv_root),
    )
    updater._recover_interrupted_run()

    assert destination.read_text(encoding="utf-8") == "old\n"
    assert not cache.exists()
    assert (staging / "RECOVERED.txt").is_file()
    status = json.loads(update_status_path(bundle_root).read_text(encoding="utf-8"))
    assert status["status"] == "failed"
    assert status["error"]["type"] == "InterruptedUpdate"


def test_missing_dataset_partitions_are_backfilled(
    csv_dir: Path, tmp_path: Path
) -> None:
    target_csv = tmp_path / "csv"
    updater = DataUpdater(
        UpdateOptions(
            csv_dir=target_csv,
            bundle_root=tmp_path / "bundle",
            lookback=0,
            retries=1,
        ),
        client=FakeProClient(csv_dir),
    )
    trade_cal = pd.read_csv(csv_dir / "trade_cal.csv", dtype=str)
    assert updater._target_dates(trade_cal, "20240108") == [
        "20240102",
        "20240103",
        "20240104",
        "20240105",
        "20240108",
    ]


def test_paginated_fetch_uses_offsets(csv_dir: Path, tmp_path: Path) -> None:
    class PagingClient:
        def __init__(self) -> None:
            self.offsets: list[int] = []

        def query(self, api_name: str, **params: Any) -> pd.DataFrame:
            self.offsets.append(int(params["offset"]))
            source = pd.DataFrame(
                {"ts_code": [f"{index:06d}.SZ" for index in range(5)]}
            )
            offset = int(params["offset"])
            limit = int(params["limit"])
            return source.iloc[offset : offset + limit].reset_index(drop=True)

    client = PagingClient()
    updater = DataUpdater(
        UpdateOptions(csv_dir=csv_dir, bundle_root=tmp_path, retries=1),
        client=client,
    )
    frame = updater._fetch_paginated("test", {}, 2)
    assert len(frame) == 5
    assert client.offsets == [0, 2, 4]
