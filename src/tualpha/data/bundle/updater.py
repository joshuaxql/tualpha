"""Tushare download to partitioned CSV and atomic Parquet publication."""

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
from tqdm.auto import tqdm

# Avoid tqdm's monitor thread during long native DuckDB/Parquet operations on Windows.
tqdm.monitor_interval = 0

from ...config import DEFAULT_BUNDLE_ROOT
from ...exceptions import ConfigurationError, DataError
from ..tushare_fields import FINANCIAL_FIELDS
from .calendar_store import normalize_trade_calendar
from .csv_cache import CSV_CACHE_PROTOCOL, CsvUpdateWriter
from .manager import BUNDLE_NAME, update_status_path, validate_bundle_name
from .parquet_schema import INDEX_DAILY_CODES
from .parquet_store import load_manifest, sha256_file
from .parquet_writer import (
    active_index_weight_state,
    active_trade_dates,
    build_parquet_bundle,
    find_active_bundle,
)

UPDATE_SCHEMA_VERSION = 6
INDEX_WEIGHT_COVERAGE_SCHEMA_VERSION = 1
DEFAULT_INDEX_WEIGHT_CODES = (
    "000300.SH",
    "000852.SH",
    "000905.SH",
    "000906.SH",
    "899050.BJ",
)
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
STOCK_BASIC_FIELDS = (
    "ts_code",
    "name",
    "market",
    "exchange",
    "list_status",
    "list_date",
    "delist_date",
)
ETF_BASIC_FIELDS = (
    "ts_code",
    "csname",
    "extname",
    "cname",
    "index_code",
    "index_name",
    "setup_date",
    "list_date",
    "list_status",
    "exchange",
    "mgr_name",
    "custod_name",
    "mgt_fee",
    "etf_type",
)
INDEX_BASIC_FIELDS = (
    "ts_code",
    "name",
    "fullname",
    "market",
    "publisher",
    "index_type",
    "category",
    "base_date",
    "base_point",
    "list_date",
    "weight_rule",
    "desc",
    "exp_date",
)
INDEX_BASIC_MARKETS = ("MSCI", "CSI", "SSE", "SZSE", "CICC", "SW", "OTH")


def _concat_frames(frames: Sequence[pd.DataFrame]) -> pd.DataFrame:
    """Concatenate API pages without pandas' empty/all-NA dtype ambiguity."""
    columns = list(
        dict.fromkeys(column for frame in frames for column in frame.columns)
    )
    populated: list[pd.DataFrame] = []
    for frame in frames:
        if frame.empty:
            continue
        useful_columns = frame.columns[~frame.isna().all(axis=0)]
        if len(useful_columns):
            populated.append(frame.loc[:, useful_columns])
    if not populated:
        return pd.DataFrame(columns=columns)
    if len(populated) == 1:
        result = populated[0].reset_index(drop=True)
    else:
        result = pd.concat(populated, ignore_index=True, sort=False)
    return result.reindex(columns=columns)


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
    bundle_root: Path = DEFAULT_BUNDLE_ROOT
    bundle_name: str = BUNDLE_NAME
    start: str | None = None
    end: str | None = None
    repair_from: str | None = None
    lookback: int = 10
    retries: int = 3
    backoff: float = 2.0
    dry_run: bool = False
    full: bool = False
    compact: bool = False
    show_progress: bool = False
    index_weight_codes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        self.bundle_root = Path(self.bundle_root).expanduser()
        try:
            validate_bundle_name(self.bundle_name)
        except DataError as exc:
            raise ConfigurationError(str(exc)) from exc
        if self.lookback < 0:
            raise ConfigurationError("lookback must be non-negative")
        if self.full and self.repair_from is not None:
            raise ConfigurationError("full and repair_from cannot be used together")
        if self.retries < 1:
            raise ConfigurationError("retries must be at least 1")
        if self.backoff < 0:
            raise ConfigurationError("backoff must be non-negative")
        self.index_weight_codes = tuple(
            dict.fromkeys(
                code.strip().upper() for code in self.index_weight_codes if code.strip()
            )
        )
        if any("." not in code for code in self.index_weight_codes):
            raise ConfigurationError(
                "index weight codes must include an exchange suffix"
            )
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


@dataclass(frozen=True, slots=True)
class ResumableRun:
    run_id: str
    started_at: str
    staging: Path


@dataclass(frozen=True, slots=True)
class MasterData:
    stock: pd.DataFrame
    etf: pd.DataFrame
    index: pd.DataFrame
    trade_cal: pd.DataFrame


class DataUpdater:
    """Download maintained datasets into temporary CSV and publish a Bundle."""

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
            f"Tushare API {api_name!r} failed after {self.options.retries} "
            f"attempts: {error}"
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
            row_hashes = pd.util.hash_pandas_object(
                frame, index=True, categorize=True
            ).to_numpy(dtype="<u8")
            fingerprint = hashlib.sha256()
            fingerprint.update(json.dumps(list(frame.columns)).encode("utf-8"))
            fingerprint.update(row_hashes.tobytes())
            digest = fingerprint.hexdigest()
            if len(frame) >= page_size:
                if digest in seen_full_pages:
                    raise DataError(
                        f"{api_name} ignored pagination; repeated a full page"
                    )
                seen_full_pages.add(digest)
            pages.append(frame)
            if len(frame) < page_size:
                break
        else:  # pragma: no cover - safety valve
            raise DataError(f"{api_name} exceeded 1000 pagination requests")
        if not pages:
            return pd.DataFrame()
        return _concat_frames(pages)

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

    def _refresh_masters(
        self,
        active_sessions: pd.DatetimeIndex,
        writer: CsvUpdateWriter | None = None,
    ) -> MasterData:
        stock_pages: list[pd.DataFrame] = []
        for status in ("L", "D", "P", "G"):
            partition = f"masters/stock_basic/{status}"
            if writer is not None and writer.has_partition(partition):
                frame = writer.read_raw_partition(partition)
            else:
                frame = self._fetch_paginated(
                    "stock_basic",
                    {
                        "list_status": status,
                        "fields": ",".join(STOCK_BASIC_FIELDS),
                    },
                    6000,
                ).reindex(columns=STOCK_BASIC_FIELDS)
                if writer is not None:
                    writer.write_raw_partition(partition, frame)
            stock_pages.append(frame)
        stock = self._normalize_frame(_concat_frames(stock_pages), ["ts_code"])
        if stock.empty:
            raise DataError("stock_basic returned no assets")
        etf_pages: list[pd.DataFrame] = []
        for status in ("L", "D", "P"):
            partition = f"masters/etf_basic/{status}"
            if writer is not None and writer.has_partition(partition):
                frame = writer.read_raw_partition(partition)
            else:
                frame = self._fetch_paginated(
                    "etf_basic",
                    {
                        "list_status": status,
                        "fields": ",".join(ETF_BASIC_FIELDS),
                    },
                    5000,
                ).reindex(columns=ETF_BASIC_FIELDS)
                if writer is not None:
                    writer.write_raw_partition(partition, frame)
            etf_pages.append(frame)
        etf = self._normalize_frame(_concat_frames(etf_pages), ["ts_code"])
        if etf.empty:
            raise DataError("etf_basic returned no assets")

        index_pages: list[pd.DataFrame] = []
        for market in INDEX_BASIC_MARKETS:
            partition = f"masters/index_basic/{market}"
            if writer is not None and writer.has_partition(partition):
                frame = writer.read_raw_partition(partition)
            else:
                frame = self._fetch_paginated(
                    "index_basic",
                    {"market": market, "fields": ",".join(INDEX_BASIC_FIELDS)},
                    8000,
                ).reindex(columns=INDEX_BASIC_FIELDS)
                if writer is not None:
                    writer.write_raw_partition(partition, frame)
            index_pages.append(frame)
        index = self._normalize_frame(_concat_frames(index_pages), ["ts_code"])
        if index.empty:
            raise DataError("index_basic returned no indices")

        today = pd.Timestamp.now(tz="Asia/Shanghai").tz_localize(None).normalize()
        requested_starts = [today - pd.DateOffset(years=1)]
        if len(active_sessions):
            requested_starts.append(
                active_sessions[0] if self.options.full else active_sessions[-1]
            )
        for value in (self.options.start, self.options.repair_from):
            if value:
                requested_starts.append(pd.Timestamp(value))
        calendar_start = min(requested_starts)
        calendar_end = max(
            today + pd.DateOffset(years=2),
            pd.Timestamp(self.options.end) if self.options.end else today,
        )
        calendar_partition = "masters/trade_cal/SSE"
        if writer is not None and writer.has_partition(calendar_partition):
            calendar_update = writer.read_raw_partition(calendar_partition)
        else:
            calendar_update = self._fetch_paginated(
                "trade_cal",
                {
                    "exchange": "SSE",
                    "start_date": calendar_start.strftime("%Y%m%d"),
                    "end_date": calendar_end.strftime("%Y%m%d"),
                },
                5000,
            )
            if writer is not None:
                writer.write_raw_partition(calendar_partition, calendar_update)
        trade_cal = normalize_trade_calendar(calendar_update)
        return MasterData(stock=stock, etf=etf, index=index, trade_cal=trade_cal)

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

    def _target_dates(
        self,
        trade_cal: pd.DataFrame,
        safe_end: str,
        active_sessions: pd.DatetimeIndex,
    ) -> list[str]:
        open_dates = sorted(
            {
                str(value)
                for value in trade_cal.loc[
                    trade_cal["is_open"].astype(str) == "1", "cal_date"
                ]
                if str(value) <= safe_end
            }
        )
        if not open_dates:
            raise DataError("trade calendar has no target sessions")
        if self.options.full:
            if self.options.start:
                start = self.options.start
            elif len(active_sessions):
                start = active_sessions[0].strftime("%Y%m%d")
            else:
                raise ConfigurationError(
                    "a full build requires an explicit --from date"
                )
        elif self.options.start:
            start = self.options.start
        elif self.options.repair_from:
            start = self.options.repair_from
        elif not len(active_sessions):
            start = open_dates[0]
        else:
            position = (
                len(open_dates) - 1
                if self.options.lookback == 0
                else max(0, len(open_dates) - self.options.lookback)
            )
            active_end = active_sessions[-1].strftime("%Y%m%d")
            start = min(active_end, open_dates[position])
        return [date for date in open_dates if start <= date <= safe_end]

    def _download_daily_partitions(
        self,
        dates: Sequence[str],
        writer: CsvUpdateWriter,
    ) -> None:
        current_partition = "masters/industry/current"
        if writer.has_partition(current_partition):
            current_industry = writer.read_raw_partition(current_partition)
        else:
            current_industry = self._fetch_paginated(
                "index_member_all", {"is_new": "Y"}, 2000
            )
            writer.write_raw_partition(current_partition, current_industry)
        historical_partition = "masters/industry/historical"
        if writer.has_partition(historical_partition):
            historical_industry = writer.read_raw_partition(historical_partition)
        else:
            historical_industry = self._fetch_paginated(
                "index_member_all", {"is_new": "N"}, 2000
            )
            writer.write_raw_partition(historical_partition, historical_industry)
        industry = _concat_frames([current_industry, historical_industry])
        industry = self._normalize_frame(industry, ["ts_code", "in_date", "out_date"])
        if dates and industry.empty:
            raise DataError("index_member_all returned no industry members")

        for date in tqdm(
            dates,
            desc="下载日频数据",
            unit="交易日",
            dynamic_ncols=True,
            leave=False,
            position=1,
            disable=not self.options.show_progress,
        ):
            for spec in DAILY_DATASETS:
                partition = f"daily/{spec.directory}/{date}"
                if writer.has_partition(partition):
                    continue
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
                            f"{spec.api_name} returned unexpected dates: "
                            f"{returned_dates}"
                        )
                if spec.directory == "daily":
                    writer.append_daily(frame, "stock", partition=partition)
                elif spec.directory == "fund_daily":
                    writer.append_daily(frame, "etf", partition=partition)
                elif spec.directory == "index_daily":
                    frame = frame[
                        frame["ts_code"].astype(str).str.upper().isin(INDEX_DAILY_CODES)
                    ]
                    writer.append_daily(frame, "index", partition=partition)
                elif spec.directory in {"adj_factor", "fund_adj"}:
                    writer.append_daily_role("adj_factor", frame, partition=partition)
                else:
                    writer.append_daily_role(spec.directory, frame, partition=partition)

            industry_partition = f"daily/industry/{date}"
            if writer.has_partition(industry_partition):
                continue
            point_in_time = industry.copy()
            point_in_time["in_date"] = point_in_time["in_date"].fillna("").astype(str)
            point_in_time["out_date"] = point_in_time["out_date"].fillna("").astype(str)
            point_in_time = point_in_time[
                ((point_in_time["in_date"] == "") | (point_in_time["in_date"] <= date))
                & (
                    (point_in_time["out_date"] == "")
                    | (point_in_time["out_date"] > date)
                )
            ]
            point_in_time = point_in_time.sort_values("in_date", kind="stable")
            point_in_time = point_in_time.drop_duplicates("ts_code", keep="last")
            required = [
                "ts_code",
                "l1_code",
                "l1_name",
                "l2_code",
                "l2_name",
                "l3_code",
                "l3_name",
            ]
            missing = set(required).difference(point_in_time.columns)
            if missing:
                raise DataError(
                    f"index_member_all is missing industry fields: {sorted(missing)}"
                )
            industry_day = point_in_time[required].copy()
            industry_day.insert(1, "trade_date", date)
            industry_day = self._normalize_frame(
                industry_day, ["ts_code", "trade_date"]
            )
            writer.append_daily_role(
                "industry",
                industry_day,
                partition=industry_partition,
            )

    @staticmethod
    def _recent_quarter_ends(end: pd.Timestamp, count: int = 8) -> list[str]:
        period = end.to_period("Q")
        if period.end_time.normalize() > end:
            period -= 1
        return [
            (period - offset).end_time.normalize().strftime("%Y%m%d")
            for offset in reversed(range(count))
        ]

    @classmethod
    def _full_financial_periods(cls, start: str, safe_end: str) -> set[str]:
        first = pd.to_datetime(start, format="%Y%m%d") - pd.DateOffset(years=2)
        end = pd.to_datetime(safe_end, format="%Y%m%d")
        last_completed = cls._recent_quarter_ends(end, count=1)[0]
        return {
            period.end_time.normalize().strftime("%Y%m%d")
            for period in pd.period_range(
                first.to_period("Q"),
                pd.Timestamp(last_completed).to_period("Q"),
                freq="Q",
            )
        }

    @staticmethod
    def _incremental_financial_periods(safe_end: str) -> list[str]:
        """Return report periods expected to be publishing on the update date."""

        end = pd.to_datetime(safe_end, format="%Y%m%d")
        year = end.year
        month_day = (end.month, end.day)
        annual = f"{year - 1}1231"
        if (1, 1) <= month_day < (4, 1):
            return [annual]
        if (4, 1) <= month_day <= (4, 30):
            return [annual, f"{year}0331"]
        if (7, 1) <= month_day <= (8, 31):
            return [f"{year}0630"]
        if (10, 1) <= month_day <= (10, 31):
            return [f"{year}0930"]
        return []

    def _download_financials(
        self,
        writer: CsvUpdateWriter,
        safe_end: str,
        full_start: str | None,
    ) -> None:
        if full_start is not None:
            periods = sorted(self._full_financial_periods(full_start, safe_end))
            requests = [
                (directory, api_name, {"period": period}, period)
                for directory, api_name in FINANCIAL_DATASETS.items()
                for period in periods
            ]
        else:
            announcement_start = (
                pd.to_datetime(safe_end, format="%Y%m%d") - pd.DateOffset(days=120)
            ).strftime("%Y%m%d")
            requests = [
                (
                    directory,
                    api_name,
                    {"start_date": announcement_start, "end_date": safe_end},
                    f"ann-{announcement_start}-{safe_end}",
                )
                for directory, api_name in FINANCIAL_DATASETS.items()
            ]
        datasets = tqdm(
            requests,
            total=len(requests),
            desc="下载财务数据",
            unit="请求",
            dynamic_ncols=True,
            leave=False,
            position=1,
            disable=not self.options.show_progress,
        )
        for directory, api_name, params, label in datasets:
            partition = f"finance/{directory}/{label}"
            if writer.has_partition(partition):
                continue
            fields = ",".join(FINANCIAL_FIELDS[directory])
            frame = self._fetch_paginated(api_name, {**params, "fields": fields}, 5000)
            if not frame.empty:
                keys = [
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
                frame = self._normalize_frame(frame, keys)
            writer.append_finance(
                directory,
                frame,
                partition=partition,
            )

    def _index_weight_history_start(
        self,
        dates: Sequence[str],
        safe_end: str,
        active_sessions: pd.DatetimeIndex,
    ) -> str:
        candidates = [str(date) for date in dates]
        if self.options.full and len(active_sessions):
            candidates.append(active_sessions[0].strftime("%Y%m%d"))
        earliest = min(candidates) if candidates else safe_end
        return (
            (pd.to_datetime(earliest, format="%Y%m%d") - pd.DateOffset(months=1))
            .replace(day=1)
            .strftime("%Y%m%d")
        )

    @staticmethod
    def _normalize_index_weight_frame(
        frame: pd.DataFrame, code: str, start: str, end: str
    ) -> pd.DataFrame:
        columns = ("index_code", "con_code", "trade_date", "weight")
        missing = set(columns).difference(frame.columns)
        if missing:
            raise DataError(f"index_weight returned missing columns: {sorted(missing)}")
        result = frame[list(columns)].copy()
        result["index_code"] = result["index_code"].astype(str).str.upper()
        result["con_code"] = result["con_code"].astype(str).str.upper()
        result["trade_date"] = result["trade_date"].astype(str)
        returned_codes = set(result["index_code"])
        if returned_codes != {code}:
            raise DataError(
                f"index_weight for {code} returned unexpected codes: "
                f"{sorted(returned_codes)}"
            )
        if not result["trade_date"].between(start, end).all():
            raise DataError(f"index_weight for {code} returned dates outside request")
        weights = pd.to_numeric(result["weight"], errors="coerce")
        if weights.isna().any() or not weights.between(0.0, 100.0).all():
            raise DataError(f"index_weight for {code} returned invalid weights")
        result["weight"] = weights.astype(float)
        return DataUpdater._normalize_frame(
            result,
            ["index_code", "con_code", "trade_date"],
        )

    @staticmethod
    def _index_weight_ranges(start: str, end: str) -> list[tuple[str, str]]:
        first = pd.to_datetime(start, format="%Y%m%d")
        last = pd.to_datetime(end, format="%Y%m%d")
        return [
            (
                max(first, pd.Timestamp(year=year, month=1, day=1)).strftime("%Y%m%d"),
                min(last, pd.Timestamp(year=year, month=12, day=31)).strftime("%Y%m%d"),
            )
            for year in range(first.year, last.year + 1)
        ]

    def _download_index_weights(
        self,
        writer: CsvUpdateWriter,
        safe_end: str,
        dates: Sequence[str],
        active_sessions: pd.DatetimeIndex,
    ) -> None:
        if self.options.full:
            existing_dates, coverage = {}, {}
        else:
            existing_dates, coverage = active_index_weight_state(
                self.options.bundle_root, self.options.bundle_name
            )
        codes = {
            *DEFAULT_INDEX_WEIGHT_CODES,
            *(code.upper() for code in self.options.index_weight_codes),
            *existing_dates,
        }
        history_start = self._index_weight_history_start(
            dates, safe_end, active_sessions
        )
        if coverage.get("schema_version") != INDEX_WEIGHT_COVERAGE_SCHEMA_VERSION:
            coverage = {
                "schema_version": INDEX_WEIGHT_COVERAGE_SCHEMA_VERSION,
                "codes": {},
            }
        coverage_codes = coverage.setdefault("codes", {})
        if not isinstance(coverage_codes, dict):
            raise DataError("active index_weight coverage is invalid")

        index_codes = tqdm(
            sorted(codes),
            desc="下载指数权重",
            unit="指数",
            dynamic_ncols=True,
            leave=False,
            position=1,
            disable=not self.options.show_progress,
        )
        for code in index_codes:
            known_dates = existing_dates.get(code, set())
            covered = coverage_codes.get(code)
            if (
                not known_dates
                or not isinstance(covered, dict)
                or str(covered.get("from", safe_end)) > history_start
            ):
                start = history_start
            else:
                latest = pd.to_datetime(max(known_dates), format="%Y%m%d")
                start = (
                    (latest - pd.DateOffset(months=2)).replace(day=1).strftime("%Y%m%d")
                )
                start = max(start, history_start)
            if self.options.repair_from is not None:
                start = max(history_start, min(start, self.options.repair_from))
            start = min(start, safe_end)
            for range_start, range_end in self._index_weight_ranges(start, safe_end):
                partition = f"index_weight/{code}/{range_start[:4]}"
                if writer.has_partition(partition):
                    cached = writer.read_partition(partition)
                    if not cached.empty:
                        existing_dates.setdefault(code, set()).update(
                            cached["snapshot_date"].astype(str)
                        )
                    continue
                frame = self._fetch_paginated(
                    "index_weight",
                    {
                        "index_code": code,
                        "start_date": range_start,
                        "end_date": range_end,
                    },
                    5000,
                )
                normalized = (
                    self._normalize_index_weight_frame(
                        frame, code, range_start, range_end
                    )
                    if not frame.empty
                    else pd.DataFrame(
                        columns=[
                            "index_code",
                            "con_code",
                            "trade_date",
                            "weight",
                        ]
                    )
                )
                writer.append_index_weights(normalized, partition=partition)
                if not normalized.empty:
                    existing_dates.setdefault(code, set()).update(
                        normalized["trade_date"].astype(str)
                    )
            previous_from = (
                str(covered.get("from"))
                if isinstance(covered, dict) and covered.get("from")
                else start
            )
            coverage_codes[code] = {
                "from": min(previous_from, start),
                "through": safe_end,
            }

        missing_defaults = [
            code for code in DEFAULT_INDEX_WEIGHT_CODES if not existing_dates.get(code)
        ]
        if missing_defaults:
            raise DataError(
                "index_weight returned no historical snapshots for default indices: "
                f"{missing_defaults}"
            )
        writer.set_index_coverage(
            {
                "schema_version": INDEX_WEIGHT_COVERAGE_SCHEMA_VERSION,
                "codes": {
                    code: coverage_codes[code] for code in sorted(coverage_codes)
                },
            }
        )

    def _recover_interrupted_run(self) -> ResumableRun | None:
        if not self.status_path.is_file():
            return None
        try:
            status = json.loads(self.status_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return None
        if status.get("operation") != "data_update" or status.get("status") not in {
            "running",
            "failed",
        }:
            return None
        if (
            os.path.normcase(str(status.get("bundle_root", "")))
            != os.path.normcase(str(self.options.bundle_root.resolve()))
            or str(status.get("bundle_name", "")) != self.options.bundle_name
        ):
            return None
        staging_value = status.get("staging_path")
        if not staging_value:
            return None
        staging = Path(str(staging_value))
        active = find_active_bundle(self.options.bundle_root, self.options.bundle_name)
        committed = (staging / "BUNDLE_PUBLISHED").is_file()
        expected_path = staging / "EXPECTED_GENERATION"
        if not committed and active is not None and expected_path.is_file():
            expected_generation = expected_path.read_text(encoding="utf-8").strip()
            committed = (
                bool(expected_generation)
                and load_manifest(active)["generation"] == expected_generation
            )

        signature_matches = all(
            (
                status.get("requested_start") == self.options.start,
                status.get("requested_end") == self.options.end,
                status.get("repair_from") == self.options.repair_from,
                int(status.get("lookback", -1)) == self.options.lookback,
                bool(status.get("dry_run")) == self.options.dry_run,
                bool(status.get("full")) == self.options.full,
                bool(status.get("compact")) == self.options.compact,
                tuple(status.get("index_weight_codes", ()))
                == self.options.index_weight_codes,
            )
        )
        cache_manifest = staging / "cache" / "cache-manifest.json"
        cache_is_resumable = False
        if signature_matches and not committed and cache_manifest.is_file():
            try:
                cache_metadata = json.loads(cache_manifest.read_text(encoding="utf-8"))
                started = pd.Timestamp(str(status.get("started_at")))
                age = pd.Timestamp.now(tz="UTC") - started
                cache_is_resumable = cache_metadata.get(
                    "protocol"
                ) == CSV_CACHE_PROTOCOL and age <= pd.Timedelta(hours=24)
            except (TypeError, ValueError, OSError):
                cache_is_resumable = False
        if cache_is_resumable:
            staging.mkdir(parents=True, exist_ok=True)
            (staging / "RECOVERED.txt").write_text(
                "resuming partitioned CSV cache\n", encoding="utf-8"
            )
            return ResumableRun(
                run_id=str(status["run_id"]),
                started_at=str(status["started_at"]),
                staging=staging,
            )

        if status.get("status") != "running":
            return None
        staging.mkdir(parents=True, exist_ok=True)
        (staging / "RECOVERED.txt").write_text(
            "bundle already published\n" if committed else "active Bundle unchanged\n",
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
                        else "previous update was interrupted; active Bundle unchanged"
                    ),
                },
                "staging_path": str(staging),
            }
        )
        return None

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
        self.state_dir.mkdir(parents=True, exist_ok=True)
        lock_path = self.state_dir / ".locks" / "update.lock"
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        lock = FileLock(str(lock_path))
        with lock:
            resumable = self._recover_interrupted_run()
            if resumable is None:
                run_id = uuid4().hex
                started_at = datetime.now(ZoneInfo("UTC")).isoformat()
                staging = self.state_dir / ".staging" / "update" / run_id
                staging.mkdir(parents=True, exist_ok=False)
            else:
                run_id = resumable.run_id
                started_at = resumable.started_at
                staging = resumable.staging
                (staging / "FAILED.txt").unlink(missing_ok=True)
            run_metadata = {
                "run_id": run_id,
                "started_at": started_at,
                "requested_start": self.options.start,
                "requested_end": self.options.end,
                "repair_from": self.options.repair_from,
                "lookback": self.options.lookback,
                "dry_run": self.options.dry_run,
                "full": self.options.full,
                "compact": self.options.compact,
                "index_weight_codes": list(self.options.index_weight_codes),
                "resumed": resumable is not None,
            }
            self._write_status(
                {
                    "status": "running",
                    "phase": "download",
                    **run_metadata,
                    "staging_path": str(staging),
                }
            )
            dates: list[str] = []
            bundle_published = False
            phase = "download"
            progress = tqdm(
                total=5,
                desc=("TuAlpha 全量构建" if self.options.full else "TuAlpha 数据更新"),
                unit="阶段",
                dynamic_ncols=True,
                disable=not self.options.show_progress,
            )
            progress.set_postfix_str("刷新基础信息与交易日历", refresh=True)
            try:
                active_sessions = active_trade_dates(
                    self.options.bundle_root, self.options.bundle_name
                )
                if (
                    self.options.full
                    and not self.options.start
                    and not len(active_sessions)
                ):
                    raise ConfigurationError(
                        "a full build requires an explicit --from date"
                    )
                full_start = (
                    self.options.start
                    or (
                        active_sessions[0].strftime("%Y%m%d")
                        if len(active_sessions)
                        else None
                    )
                    if self.options.full
                    else None
                )
                cache_path = staging / "cache"
                with CsvUpdateWriter(
                    cache_path, resume=resumable is not None
                ) as writer:
                    masters = self._refresh_masters(active_sessions, writer)
                    safe_end = self._safe_end_date(masters.trade_cal)
                    dates = self._target_dates(
                        masters.trade_cal, safe_end, active_sessions
                    )
                    if not dates:
                        raise DataError("update request contains no open trading dates")
                    writer.set_target_dates(dates)
                    progress.update(1)
                    progress.set_postfix_str(
                        f"下载 {len(dates)} 个交易日的日频数据", refresh=True
                    )
                    self._download_daily_partitions(dates, writer)
                    progress.update(1)
                    progress.set_postfix_str("下载 PIT 财务数据", refresh=True)
                    financial_full_start = full_start or (
                        dates[0] if not len(active_sessions) else None
                    )
                    self._download_financials(writer, safe_end, financial_full_start)
                    progress.update(1)
                    progress.set_postfix_str("下载 PIT 指数权重", refresh=True)
                    self._download_index_weights(
                        writer,
                        safe_end,
                        dates,
                        (
                            pd.DatetimeIndex([])
                            if self.options.full
                            else active_sessions
                        ),
                    )
                    progress.update(1)
                    progress.set_postfix_str(
                        "整理暂存数据"
                        if self.options.dry_run
                        else "构建、校验并发布 Bundle",
                        refresh=True,
                    )
                    batch_count = writer.batch_count
                (staging / "journal.json").write_text(
                    json.dumps(
                        {
                            "storage": "partitioned-csv-cache",
                            "downloaded_batches": batch_count,
                            "updated_dates": dates,
                            "cache_manifest_sha256": sha256_file(
                                cache_path / "cache-manifest.json"
                            ),
                        },
                        ensure_ascii=False,
                        indent=2,
                    ),
                    encoding="utf-8",
                )
                if self.options.dry_run:
                    result = UpdateResult(
                        run_id=run_id,
                        updated_dates=tuple(dates),
                        updated_files=0,
                        bundle_path=None,
                        dry_run=True,
                    )
                    self._write_status(
                        {
                            "status": "dry_run_succeeded",
                            "phase": "download",
                            **run_metadata,
                            "completed_at": datetime.now(ZoneInfo("UTC")).isoformat(),
                            "downloaded_batches": batch_count,
                            "result": asdict(result),
                        }
                    )
                    shutil.rmtree(staging, ignore_errors=True)
                    progress.update(1)
                    progress.set_postfix_str("完成", refresh=True)
                    return result

                phase = "build"
                self._write_status(
                    {
                        "status": "running",
                        "phase": phase,
                        **run_metadata,
                        "staging_path": str(staging),
                        "downloaded_batches": batch_count,
                    }
                )
                build_arguments = {
                    "stock": masters.stock,
                    "etf": masters.etf,
                    "index": masters.index,
                    "trade_cal": masters.trade_cal,
                    "bundle_root": self.options.bundle_root,
                    "bundle_name": self.options.bundle_name,
                    "staging_root": staging,
                }
                direct = build_parquet_bundle(
                    cache_path,
                    **build_arguments,
                    merge_active=bool(len(active_sessions)) and not self.options.full,
                    force_compact=self.options.compact,
                )
                bundle_published = True
                phase = "reopen"
                (staging / "BUNDLE_PUBLISHED").write_text(
                    str(direct.result.path), encoding="utf-8"
                )
                result = UpdateResult(
                    run_id=run_id,
                    updated_dates=tuple(dates),
                    updated_files=len(direct.manifest["files"]),
                    bundle_path=str(direct.result.path),
                    dry_run=False,
                )
                active_manifest = load_manifest(direct.result.path)
                completed_at = datetime.now(ZoneInfo("UTC")).isoformat()
                self._write_status(
                    {
                        "status": "succeeded",
                        "phase": "reopen",
                        "active_generation": active_manifest["generation"],
                        "storage": "parquet+duckdb",
                        "bundle_schema": active_manifest["schema_version"],
                        "build_pipeline": direct.manifest["build_pipeline"],
                        "verification": {
                            "structural": "passed",
                            "sha256": "passed",
                            "reader": "passed",
                        },
                        **run_metadata,
                        "completed_at": completed_at,
                        "downloaded_batches": batch_count,
                        "result": asdict(result),
                        "last_success": {
                            "run_id": run_id,
                            "completed_at": completed_at,
                            "updated_dates": dates,
                            "updated_files": len(direct.manifest["files"]),
                            "bundle_path": str(direct.result.path),
                        },
                    }
                )
                shutil.rmtree(staging, ignore_errors=True)
                progress.update(1)
                progress.set_postfix_str("完成", refresh=True)
                return result
            except Exception as exc:
                progress.set_postfix_str(f"失败：{phase}", refresh=True)
                bundle_published = (
                    bundle_published or (staging / "BUNDLE_PUBLISHED").is_file()
                )
                failed_phase = "reopen" if bundle_published else phase
                (staging / "FAILED.txt").write_text(
                    f"{type(exc).__name__}: {exc}", encoding="utf-8"
                )
                self._write_status(
                    {
                        "status": "failed",
                        "phase": failed_phase,
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
                    f"update run {run_id} failed during {failed_phase}: "
                    f"{type(exc).__name__}: {exc}; staging retained at {staging}"
                ) from exc
            finally:
                progress.close()


def token_from_stdin() -> str:
    if sys.stdin.isatty():
        import getpass

        return getpass.getpass("Tushare token: ")
    return sys.stdin.readline().strip()
