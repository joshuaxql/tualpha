"""Incremental Tushare download and bundle publication workflow."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sys
import time
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol
from uuid import uuid4
from zoneinfo import ZoneInfo

import pandas as pd
from filelock import FileLock

from .bundle import (
    BUNDLE_NAME,
    NormalizedStore,
    build_bundle,
    normalized_store_path,
    paths_overlap,
    update_status_path,
    validate_bundle_name,
)
from .config import DEFAULT_BUNDLE_ROOT
from .exceptions import ConfigurationError, DataError
from .tushare_fields import FINANCIAL_FIELDS

UPDATE_SCHEMA_VERSION = 2
DAILY_KEY_COLUMNS = ("ts_code", "trade_date")


@dataclass(frozen=True, slots=True)
class DailyDataset:
    directory: str
    api_name: str
    page_size: int
    allow_empty: bool = False
    key_columns: tuple[str, ...] = DAILY_KEY_COLUMNS


DAILY_DATASETS = (
    DailyDataset("daily", "daily", 6000),
    DailyDataset("adj_factor", "adj_factor", 6000),
    DailyDataset("fund_daily", "fund_daily", 5000),
    DailyDataset("fund_adj", "fund_adj", 2000),
    DailyDataset("daily_basic", "daily_basic", 6000),
    DailyDataset("moneyflow", "moneyflow", 6000),
    DailyDataset("stk_limit", "stk_limit", 5800),
    DailyDataset("suspend_d", "suspend_d", 5000, allow_empty=True),
    DailyDataset("stock_st", "stock_st", 1000, allow_empty=True),
    DailyDataset("index_daily", "index_daily", 6000),
)
FINANCIAL_DATASETS = {
    "balancesheet": "balancesheet_vip",
    "income": "income_vip",
    "cashflow": "cashflow_vip",
    "fina_indicator": "fina_indicator_vip",
}
EMPTY_COLUMNS = {
    "suspend_d": ["ts_code", "trade_date", "suspend_timing", "suspend_type"],
    "stock_st": ["ts_code", "name", "trade_date", "type", "type_name"],
}


class ProClient(Protocol):
    def query(self, api_name: str, **params: Any) -> pd.DataFrame: ...


class TushareProClient:
    """Small wrapper that never persists or logs a Tushare token."""

    def __init__(self, token: str) -> None:
        if not token.strip():
            raise ConfigurationError("TUSHARE_TOKEN is required")
        import tushare as ts

        self._client = ts.pro_api(token.strip())

    def query(self, api_name: str, **params: Any) -> pd.DataFrame:
        return self._client.query(api_name, **params)


@dataclass(slots=True)
class UpdateOptions:
    csv_dir: Path
    bundle_root: Path = DEFAULT_BUNDLE_ROOT
    bundle_name: str = BUNDLE_NAME
    start: str | None = None
    end: str | None = None
    repair_from: str | None = None
    lookback: int = 10
    retries: int = 3
    backoff: float = 2.0
    dry_run: bool = False
    show_progress: bool = False
    index_weight_codes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        self.csv_dir = Path(self.csv_dir)
        self.bundle_root = Path(self.bundle_root).expanduser()
        try:
            validate_bundle_name(self.bundle_name)
        except DataError as exc:
            raise ConfigurationError(str(exc)) from exc
        if paths_overlap(self.csv_dir, self.bundle_root):
            raise ConfigurationError("csv_dir and bundle_root must not overlap")
        if self.lookback < 0:
            raise ConfigurationError("lookback must be non-negative")
        if self.retries < 1:
            raise ConfigurationError("retries must be at least 1")
        if self.backoff < 0:
            raise ConfigurationError("backoff must be non-negative")
        for value in (self.start, self.end, self.repair_from):
            if value is not None:
                pd.to_datetime(value, format="%Y%m%d")


@dataclass(frozen=True, slots=True)
class UpdateResult:
    run_id: str
    updated_dates: tuple[str, ...]
    updated_files: int
    bundle_path: str | None
    dry_run: bool


class DataUpdater:
    """Download all maintained datasets, then replace the current bundle."""

    def __init__(
        self,
        options: UpdateOptions,
        *,
        token: str | None = None,
        client: ProClient | None = None,
    ) -> None:
        self.options = options
        resolved_token = token or os.environ.get("TUSHARE_TOKEN", "")
        if client is None and not resolved_token:
            raise ConfigurationError(
                "TUSHARE_TOKEN is not set; data update was aborted"
            )
        self.client = client or TushareProClient(resolved_token)
        self.state_dir = options.bundle_root
        self.status_path = update_status_path(options.bundle_root)

    def _query_with_retry(self, api_name: str, params: dict[str, Any]) -> pd.DataFrame:
        error: Exception | None = None
        for attempt in range(self.options.retries):
            try:
                frame = self.client.query(api_name, **params)
                if not isinstance(frame, pd.DataFrame):
                    raise DataError(f"{api_name} returned a non-DataFrame result")
                return frame
            except Exception as exc:  # noqa: BLE001 - remote SDK raises mixed types
                error = exc
                if attempt + 1 < self.options.retries:
                    time.sleep(self.options.backoff * (2**attempt))
        raise DataError(
            f"Tushare API {api_name!r} failed after {self.options.retries} attempts: {error}"
        ) from error

    def _fetch_paginated(
        self,
        api_name: str,
        params: dict[str, Any],
        page_size: int,
    ) -> pd.DataFrame:
        pages: list[pd.DataFrame] = []
        seen_full_pages: set[str] = set()
        for page_number in range(1000):
            query = {**params, "offset": page_number * page_size, "limit": page_size}
            frame = self._query_with_retry(api_name, query)
            if frame.empty:
                break
            frame = frame.copy()
            fingerprint = hashlib.sha256(
                frame.to_csv(index=False).encode("utf-8")
            ).hexdigest()
            if len(frame) >= page_size:
                if fingerprint in seen_full_pages:
                    raise DataError(
                        f"{api_name} ignored pagination; repeated a full page"
                    )
                seen_full_pages.add(fingerprint)
            pages.append(frame)
            if len(frame) < page_size:
                break
        else:  # pragma: no cover - safety valve
            raise DataError(f"{api_name} exceeded 1000 pagination requests")
        if not pages:
            return pd.DataFrame()
        return pd.concat(pages, ignore_index=True, sort=False)

    @staticmethod
    def _normalize_frame(
        frame: pd.DataFrame,
        key_columns: Sequence[str],
        *,
        expected_columns: Sequence[str] = (),
    ) -> pd.DataFrame:
        if frame.empty and not len(frame.columns) and expected_columns:
            frame = pd.DataFrame(columns=list(expected_columns))
        missing = set(key_columns).difference(frame.columns)
        if missing and not frame.empty:
            raise DataError(
                f"downloaded data is missing key columns: {sorted(missing)}"
            )
        if not frame.empty:
            frame = frame.drop_duplicates(list(key_columns), keep="last")
            frame = frame.sort_values(list(key_columns), kind="stable")
        return frame.reset_index(drop=True)

    @staticmethod
    def _stage_csv(frame: pd.DataFrame, path: Path) -> dict[str, Any]:
        path.parent.mkdir(parents=True, exist_ok=True)
        frame.to_csv(path, index=False, encoding="utf-8")
        content = path.read_bytes()
        return {
            "path": str(path),
            "rows": len(frame),
            "sha256": hashlib.sha256(content).hexdigest(),
        }

    def _refresh_masters(
        self, staging: Path
    ) -> tuple[pd.DataFrame, list[tuple[Path, Path]], list[dict[str, Any]]]:
        publications: list[tuple[Path, Path]] = []
        journal: list[dict[str, Any]] = []

        stock_pages = [
            self._fetch_paginated("stock_basic", {"list_status": status}, 6000)
            for status in ("L", "D", "P", "G")
        ]
        stock = self._normalize_frame(
            pd.concat(stock_pages, ignore_index=True, sort=False), ["ts_code"]
        )
        if stock.empty:
            raise DataError("stock_basic returned no assets")
        etf_pages = [
            self._fetch_paginated("etf_basic", {"list_status": status}, 5000)
            for status in ("L", "D", "P")
        ]
        etf = self._normalize_frame(
            pd.concat(etf_pages, ignore_index=True, sort=False), ["ts_code"]
        )
        if etf.empty:
            raise DataError("etf_basic returned no assets")
        index_pages = [
            self._fetch_paginated("index_basic", {"market": market}, 5000)
            for market in ("SSE", "SZSE", "CSI")
        ]
        index_basic = pd.concat(index_pages, ignore_index=True, sort=False)
        existing_index_path = self.options.csv_dir / "index_basic.csv"
        if existing_index_path.is_file():
            existing_index = pd.read_csv(
                existing_index_path, dtype=str, keep_default_na=False
            )
            index_basic = pd.concat(
                [existing_index, index_basic], ignore_index=True, sort=False
            )
        index_basic = self._normalize_frame(index_basic, ["ts_code"])
        if index_basic.empty:
            raise DataError("index_basic returned no indices")

        today = pd.Timestamp.now(tz="Asia/Shanghai").tz_localize(None).normalize()
        calendar_start = today - pd.DateOffset(years=1)
        calendar_end = today + pd.DateOffset(years=2)
        calendar_update = self._fetch_paginated(
            "trade_cal",
            {
                "exchange": "SSE",
                "start_date": calendar_start.strftime("%Y%m%d"),
                "end_date": calendar_end.strftime("%Y%m%d"),
            },
            5000,
        )
        existing_calendar_path = self.options.csv_dir / "trade_cal.csv"
        if existing_calendar_path.is_file():
            existing_calendar = pd.read_csv(
                existing_calendar_path, dtype=str, keep_default_na=False
            )
            calendar_update = pd.concat(
                [existing_calendar, calendar_update], ignore_index=True, sort=False
            )
        trade_cal = self._normalize_frame(calendar_update, ["exchange", "cal_date"])
        if trade_cal.empty:
            raise DataError("trade_cal returned no calendar rows")

        for filename, frame in (
            ("stock_basic.csv", stock),
            ("etf_basic.csv", etf),
            ("index_basic.csv", index_basic),
            ("trade_cal.csv", trade_cal),
        ):
            staged = staging / filename
            journal.append(self._stage_csv(frame, staged))
            publications.append((staged, self.options.csv_dir / filename))
        return trade_cal, publications, journal

    def _safe_end_date(self, trade_cal: pd.DataFrame) -> str:
        today = pd.Timestamp.now(tz="Asia/Shanghai").strftime("%Y%m%d")
        open_dates = sorted(
            {
                str(value)
                for value in trade_cal.loc[
                    trade_cal["is_open"].astype(str) == "1", "cal_date"
                ]
                if str(value) < today
            }
        )
        if not open_dates:
            raise DataError("trade calendar has no completed open session")
        requested = self.options.end or open_dates[-1]
        candidates = [date for date in open_dates if date <= requested]
        if not candidates:
            raise DataError(f"no completed trading session on or before {requested}")
        return candidates[-1]

    def _target_dates(self, trade_cal: pd.DataFrame, safe_end: str) -> list[str]:
        open_dates = sorted(
            {
                str(value)
                for value in trade_cal.loc[
                    trade_cal["is_open"].astype(str) == "1", "cal_date"
                ]
                if str(value) <= safe_end
            }
        )
        explicit_range = bool(self.options.start or self.options.repair_from)
        if self.options.start:
            start = self.options.start
        elif self.options.repair_from:
            start = self.options.repair_from
        else:
            existing = sorted((self.options.csv_dir / "daily").glob("*.csv"))
            last_existing = existing[-1].stem if existing else open_dates[0]
            position = min(
                max(0, len(open_dates) - self.options.lookback),
                max(0, len(open_dates) - 1),
            )
            lookback_start = open_dates[position] if open_dates else last_existing
            start = min(last_existing, lookback_start)
        targets = {date for date in open_dates if start <= date <= safe_end}
        if not explicit_range:
            required_directories = {
                *(spec.directory for spec in DAILY_DATASETS),
                "industry",
            }
            targets.update(
                date
                for date in open_dates
                if any(
                    not (self.options.csv_dir / directory / f"{date}.csv").is_file()
                    for directory in required_directories
                )
            )
        return sorted(targets)

    def _download_daily_partitions(
        self,
        dates: Sequence[str],
        staging: Path,
    ) -> tuple[list[tuple[Path, Path]], list[dict[str, Any]]]:
        publications: list[tuple[Path, Path]] = []
        journal: list[dict[str, Any]] = []
        current_industry = self._fetch_paginated(
            "index_member_all", {"is_new": "Y"}, 2000
        )
        historical_industry = self._fetch_paginated(
            "index_member_all", {"is_new": "N"}, 2000
        )
        industry = pd.concat(
            [current_industry, historical_industry], ignore_index=True, sort=False
        )
        industry = self._normalize_frame(industry, ["ts_code", "in_date", "out_date"])
        if dates and industry.empty:
            raise DataError("index_member_all returned no industry members")

        for date in dates:
            for spec in DAILY_DATASETS:
                frame = self._fetch_paginated(
                    spec.api_name, {"trade_date": date}, spec.page_size
                )
                if frame.empty and not spec.allow_empty:
                    raise DataError(
                        f"required dataset {spec.api_name} is empty for open date {date}"
                    )
                frame = self._normalize_frame(
                    frame,
                    spec.key_columns,
                    expected_columns=EMPTY_COLUMNS.get(spec.directory, ()),
                )
                if not frame.empty and "trade_date" in frame:
                    returned_dates = set(frame["trade_date"].astype(str))
                    if returned_dates != {date}:
                        raise DataError(
                            f"{spec.api_name} returned unexpected dates: {returned_dates}"
                        )
                staged = staging / spec.directory / f"{date}.csv"
                entry = self._stage_csv(frame, staged)
                entry.update({"dataset": spec.directory, "trade_date": date})
                journal.append(entry)
                publications.append(
                    (staged, self.options.csv_dir / spec.directory / f"{date}.csv")
                )

            industry_destination = self.options.csv_dir / "industry" / f"{date}.csv"
            if not industry_destination.exists() or self.options.repair_from:
                point_in_time = industry.copy()
                point_in_time["in_date"] = (
                    point_in_time["in_date"].fillna("").astype(str)
                )
                point_in_time["out_date"] = (
                    point_in_time["out_date"].fillna("").astype(str)
                )
                point_in_time = point_in_time[
                    (
                        (point_in_time["in_date"] == "")
                        | (point_in_time["in_date"] <= date)
                    )
                    & (
                        (point_in_time["out_date"] == "")
                        | (point_in_time["out_date"] > date)
                    )
                ]
                point_in_time = point_in_time.sort_values("in_date", kind="stable")
                point_in_time = point_in_time.drop_duplicates("ts_code", keep="last")
                industry_day = point_in_time[
                    [
                        "ts_code",
                        "l1_code",
                        "l1_name",
                        "l2_code",
                        "l2_name",
                        "l3_code",
                        "l3_name",
                    ]
                ].copy()
                industry_day.insert(1, "trade_date", date)
                industry_day = self._normalize_frame(
                    industry_day, ["ts_code", "trade_date"]
                )
                staged = staging / "industry" / f"{date}.csv"
                journal.append(self._stage_csv(industry_day, staged))
                publications.append((staged, industry_destination))
        return publications, journal

    @staticmethod
    def _recent_quarter_ends(today: pd.Timestamp, count: int = 8) -> list[str]:
        period = today.to_period("Q")
        if period.end_time.normalize() > today:
            period -= 1
        return [
            (period - offset).end_time.normalize().strftime("%Y%m%d")
            for offset in reversed(range(count))
        ]

    def _financial_revision_start(self, directory: str, safe_end: str) -> str:
        latest: pd.Timestamp | None = None
        for path in (self.options.csv_dir / directory).glob("*.csv"):
            try:
                frame = pd.read_csv(
                    path,
                    dtype=str,
                    usecols=lambda column: column in {"ann_date", "f_ann_date"},
                )
            except (ValueError, pd.errors.EmptyDataError):
                continue
            for column in ("ann_date", "f_ann_date"):
                if column not in frame:
                    continue
                values = pd.to_datetime(
                    frame[column], format="%Y%m%d", errors="coerce"
                ).dropna()
                if not values.empty:
                    candidate = values.max()
                    latest = candidate if latest is None else max(latest, candidate)
        end = pd.to_datetime(safe_end, format="%Y%m%d")
        start = (
            end - pd.DateOffset(years=2)
            if latest is None
            else latest - pd.Timedelta(days=30)
        )
        return min(start, end).strftime("%Y%m%d")

    def _download_financials(
        self, staging: Path, safe_end: str
    ) -> tuple[list[tuple[Path, Path]], list[dict[str, Any]]]:
        publications: list[tuple[Path, Path]] = []
        journal: list[dict[str, Any]] = []
        recent_periods = set(
            self._recent_quarter_ends(
                pd.Timestamp.now(tz="Asia/Shanghai").tz_localize(None).normalize()
            )
        )

        for directory, api_name in FINANCIAL_DATASETS.items():
            existing = sorted((self.options.csv_dir / directory).glob("*.csv"))
            periods = recent_periods | {path.stem for path in existing[-8:]}
            expected_fields = set(FINANCIAL_FIELDS[directory])
            for path in existing:
                try:
                    columns = set(pd.read_csv(path, nrows=0).columns)
                except pd.errors.EmptyDataError:
                    columns = set()
                if not expected_fields.issubset(columns):
                    periods.add(path.stem)
            if directory == "fina_indicator":
                # This endpoint's start/end dates refer to report periods, not
                # announcement dates, so all known periods must be refreshed
                # to discover old-period revisions.
                periods.update(path.stem for path in existing)

            fields = ",".join(FINANCIAL_FIELDS[directory])
            fetched_by_period: dict[str, list[pd.DataFrame]] = {}
            for period in sorted(periods):
                frame = self._fetch_paginated(
                    api_name, {"period": period, "fields": fields}, 5000
                )
                if not frame.empty:
                    fetched_by_period.setdefault(period, []).append(frame)
            revisions = (
                pd.DataFrame()
                if directory == "fina_indicator"
                else self._fetch_paginated(
                    api_name,
                    {
                        "start_date": self._financial_revision_start(
                            directory, safe_end
                        ),
                        "end_date": safe_end,
                        "fields": fields,
                    },
                    5000,
                )
            )
            if not revisions.empty:
                if "end_date" not in revisions:
                    raise DataError(f"{api_name} revisions are missing end_date")
                for period, frame in revisions.groupby("end_date"):
                    fetched_by_period.setdefault(str(period), []).append(frame)

            for period, frames in sorted(fetched_by_period.items()):
                destination = self.options.csv_dir / directory / f"{period}.csv"
                if destination.is_file():
                    frames.insert(
                        0,
                        pd.read_csv(destination, dtype=str, keep_default_na=False),
                    )
                frame = pd.concat(frames, ignore_index=True, sort=False)
                key_columns = [
                    column
                    for column in (
                        "ts_code",
                        "end_date",
                        "ann_date",
                        "f_ann_date",
                        "report_type",
                        "comp_type",
                        "end_type",
                        "update_flag",
                    )
                    if column in frame.columns
                ]
                frame = self._normalize_frame(frame, key_columns)
                staged = staging / directory / f"{period}.csv"
                entry = self._stage_csv(frame, staged)
                entry.update({"dataset": directory, "period": period})
                journal.append(entry)
                publications.append((staged, destination))
        return publications, journal

    def _download_index_weights(
        self, staging: Path, safe_end: str
    ) -> tuple[list[tuple[Path, Path]], list[dict[str, Any]]]:
        codes = set(self.options.index_weight_codes)
        for path in (self.options.csv_dir / "index_weight").glob("*.csv"):
            try:
                frame = pd.read_csv(path, usecols=["index_code"], dtype=str)
            except (ValueError, pd.errors.EmptyDataError):
                continue
            codes.update(frame["index_code"].dropna().astype(str))
        if not codes:
            return [], []
        end = pd.Timestamp(safe_end)
        start = end.replace(day=1).strftime("%Y%m%d")
        frames = [
            self._fetch_paginated(
                "index_weight",
                {"index_code": code, "start_date": start, "end_date": safe_end},
                5000,
            )
            for code in sorted(codes)
        ]
        combined = pd.concat(frames, ignore_index=True, sort=False)
        if combined.empty:
            return [], []
        publications = []
        journal = []
        for trade_date, frame in combined.groupby("trade_date"):
            date = str(trade_date)
            frame = self._normalize_frame(
                frame, ["index_code", "con_code", "trade_date"]
            )
            staged = staging / "index_weight" / f"{date}.csv"
            journal.append(self._stage_csv(frame, staged))
            publications.append(
                (staged, self.options.csv_dir / "index_weight" / f"{date}.csv")
            )
        return publications, journal

    @staticmethod
    def _restore_files(backups: Sequence[tuple[Path, Path, bool]]) -> None:
        for destination, backup, existed in reversed(backups):
            destination.unlink(missing_ok=True)
            if existed and backup.exists():
                destination.parent.mkdir(parents=True, exist_ok=True)
                os.replace(backup, destination)

    @classmethod
    def _publish_files(
        cls,
        publications: Sequence[tuple[Path, Path]],
        backup_root: Path,
    ) -> list[tuple[Path, Path, bool]]:
        backups: list[tuple[Path, Path, bool]] = []
        backup_root.mkdir(parents=True, exist_ok=True)
        manifest_path = backup_root / "manifest.json"
        entries: list[dict[str, Any]] = []

        def write_manifest() -> None:
            temporary = manifest_path.with_suffix(".tmp")
            temporary.write_text(
                json.dumps(entries, ensure_ascii=False, indent=2), encoding="utf-8"
            )
            os.replace(temporary, manifest_path)

        try:
            for index, (staged, destination) in enumerate(publications):
                destination.parent.mkdir(parents=True, exist_ok=True)
                backup = backup_root / f"{index:06d}.csv"
                existed = destination.exists()
                backups.append((destination, backup, existed))
                entry = {
                    "staged": str(staged.resolve()),
                    "destination": str(destination.resolve()),
                    "backup": str(backup.resolve()),
                    "existed": existed,
                    "phase": "planned",
                }
                entries.append(entry)
                write_manifest()
                if existed:
                    os.replace(destination, backup)
                    entry["phase"] = "backed_up"
                    write_manifest()
                os.replace(staged, destination)
                entry["phase"] = "published"
                write_manifest()
        except Exception:
            cls._restore_files(backups)
            raise
        return backups

    def _recover_interrupted_run(self) -> None:
        if not self.status_path.is_file():
            return
        try:
            status = json.loads(self.status_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return
        if status.get("status") != "running":
            return
        if os.path.normcase(str(status.get("csv_dir", ""))) != os.path.normcase(
            str(self.options.csv_dir.resolve())
        ):
            return
        staging_value = status.get("staging_path")
        if not staging_value:
            return
        staging = Path(str(staging_value))
        manifest_path = staging / "backups" / "manifest.json"
        committed = (staging / "BUNDLE_PUBLISHED").is_file()
        restored = False
        if manifest_path.is_file() and not committed:
            entries = json.loads(manifest_path.read_text(encoding="utf-8"))
            for entry in reversed(entries):
                destination = Path(entry["destination"])
                backup = Path(entry["backup"])
                staged = Path(entry["staged"])
                changed = (
                    entry.get("phase") in {"backed_up", "published"}
                    or backup.exists()
                    or not staged.exists()
                )
                if not changed:
                    continue
                destination.unlink(missing_ok=True)
                if bool(entry.get("existed")) and backup.exists():
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    os.replace(backup, destination)
                restored = True
            if restored:
                cache = normalized_store_path(
                    self.options.bundle_root, self.options.bundle_name
                )
                cache.unlink(missing_ok=True)
                Path(f"{cache}.wal").unlink(missing_ok=True)
        staging.mkdir(parents=True, exist_ok=True)
        (staging / "RECOVERED.txt").write_text(
            "bundle already published\n" if committed else "CSV files restored\n",
            encoding="utf-8",
        )
        self._write_status(
            {
                "status": "failed",
                "run_id": status.get("run_id"),
                "started_at": status.get("started_at"),
                "completed_at": datetime.now(ZoneInfo("UTC")).isoformat(),
                "error": {
                    "type": "InterruptedUpdate",
                    "message": (
                        "previous update was interrupted after Bundle publication"
                        if committed
                        else "previous update was interrupted; CSV files restored"
                    ),
                },
                "staging_path": str(staging),
            }
        )

    def _write_status(self, payload: dict[str, Any]) -> None:
        previous: dict[str, Any] = {}
        if self.status_path.is_file():
            try:
                previous = json.loads(self.status_path.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                previous = {}
        status = {
            "schema_version": UPDATE_SCHEMA_VERSION,
            "operation": "data_update",
            "bundle_name": self.options.bundle_name,
            "csv_dir": str(self.options.csv_dir.resolve()),
            "bundle_root": str(self.options.bundle_root.resolve()),
            **payload,
        }
        for key in ("last_success", "last_bundle_build"):
            if key not in status and key in previous:
                status[key] = previous[key]
        self.state_dir.mkdir(parents=True, exist_ok=True)
        temporary = self.status_path.with_suffix(".tmp")
        temporary.write_text(
            json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        os.replace(temporary, self.status_path)

    def run(self) -> UpdateResult:
        self.options.csv_dir.mkdir(parents=True, exist_ok=True)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        lock_path = self.state_dir / ".locks" / "update.lock"
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        lock = FileLock(str(lock_path))
        with lock:
            self._recover_interrupted_run()
            run_id = uuid4().hex
            started_at = datetime.now(ZoneInfo("UTC")).isoformat()
            staging = self.state_dir / ".staging" / "update" / run_id
            staging.mkdir(parents=True, exist_ok=False)
            run_metadata = {
                "run_id": run_id,
                "started_at": started_at,
                "requested_start": self.options.start,
                "requested_end": self.options.end,
                "repair_from": self.options.repair_from,
                "lookback": self.options.lookback,
                "dry_run": self.options.dry_run,
            }
            self._write_status(
                {
                    "status": "running",
                    **run_metadata,
                    "staging_path": str(staging),
                }
            )
            publications: list[tuple[Path, Path]] = []
            journal: list[dict[str, Any]] = []
            backups: list[tuple[Path, Path, bool]] = []
            store: NormalizedStore | None = None
            bundle_published = False
            dates: list[str] = []
            try:
                trade_cal, master_files, master_journal = self._refresh_masters(staging)
                publications.extend(master_files)
                journal.extend(master_journal)
                safe_end = self._safe_end_date(trade_cal)
                dates = self._target_dates(trade_cal, safe_end)
                daily_files, daily_journal = self._download_daily_partitions(
                    dates, staging
                )
                publications.extend(daily_files)
                journal.extend(daily_journal)
                financial_files, financial_journal = self._download_financials(
                    staging, safe_end
                )
                publications.extend(financial_files)
                journal.extend(financial_journal)
                weight_files, weight_journal = self._download_index_weights(
                    staging, safe_end
                )
                publications.extend(weight_files)
                journal.extend(weight_journal)
                (staging / "journal.json").write_text(
                    json.dumps(journal, ensure_ascii=False, indent=2), encoding="utf-8"
                )
                if self.options.dry_run:
                    result = UpdateResult(
                        run_id=run_id,
                        updated_dates=tuple(dates),
                        updated_files=len(publications),
                        bundle_path=None,
                        dry_run=True,
                    )
                    self._write_status(
                        {
                            "status": "dry_run_succeeded",
                            **run_metadata,
                            "completed_at": datetime.now(ZoneInfo("UTC")).isoformat(),
                            "result": asdict(result),
                        }
                    )
                    shutil.rmtree(staging, ignore_errors=True)
                    return result

                backups = self._publish_files(publications, staging / "backups")
                store = NormalizedStore(
                    self.options.csv_dir,
                    self.options.bundle_root,
                    self.options.bundle_name,
                )
                store.sync_dates(dates)
                bundle = build_bundle(
                    self.options.csv_dir,
                    bundle_root=self.options.bundle_root,
                    bundle_name=self.options.bundle_name,
                    show_progress=self.options.show_progress,
                    record_status=False,
                    _maintenance_locked=True,
                )
                bundle_published = True
                (staging / "BUNDLE_PUBLISHED").write_text(
                    str(bundle.path), encoding="utf-8"
                )
                result = UpdateResult(
                    run_id=run_id,
                    updated_dates=tuple(dates),
                    updated_files=len(publications),
                    bundle_path=str(bundle.path),
                    dry_run=False,
                )
                completed_at = datetime.now(ZoneInfo("UTC")).isoformat()
                self._write_status(
                    {
                        "status": "succeeded",
                        **run_metadata,
                        "completed_at": completed_at,
                        "result": asdict(result),
                        "last_success": {
                            "run_id": run_id,
                            "completed_at": completed_at,
                            "updated_dates": dates,
                            "updated_files": len(publications),
                            "bundle_path": str(bundle.path),
                        },
                    }
                )
                shutil.rmtree(staging, ignore_errors=True)
                return result
            except Exception as exc:
                if backups and not bundle_published:
                    self._restore_files(backups)
                    if store is not None and dates:
                        try:
                            store.sync_dates(dates)
                        except Exception as rollback_error:  # noqa: BLE001
                            (staging / "ROLLBACK_FAILED.txt").write_text(
                                f"{type(rollback_error).__name__}: {rollback_error}",
                                encoding="utf-8",
                            )
                (staging / "FAILED.txt").write_text(
                    f"{type(exc).__name__}: {exc}", encoding="utf-8"
                )
                self._write_status(
                    {
                        "status": "failed",
                        **run_metadata,
                        "completed_at": datetime.now(ZoneInfo("UTC")).isoformat(),
                        "updated_dates": dates,
                        "error": {
                            "type": type(exc).__name__,
                            "message": str(exc),
                        },
                        "staging_path": str(staging),
                    }
                )
                raise DataError(
                    f"update run {run_id} failed; staging retained at {staging}"
                ) from exc


def token_from_stdin() -> str:
    if sys.stdin.isatty():
        import getpass

        return getpass.getpass("Tushare token: ")
    return sys.stdin.readline().strip()
