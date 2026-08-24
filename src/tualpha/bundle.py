"""Zipline-compatible bundle ingestion for local Tushare data."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import sqlite3
from collections.abc import Iterator, Sequence
from contextlib import ExitStack
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Any, Self
from uuid import uuid4
from zoneinfo import ZoneInfo

import bcolz
import duckdb
import numpy as np
import pandas as pd
from filelock import FileLock
from zipline.assets import ASSET_DB_VERSION, AssetDBWriter
from zipline.assets import AssetFinder as ZiplineAssetFinder
from zipline.data.adjustments import SQLiteAdjustmentReader, SQLiteAdjustmentWriter
from zipline.data.bcolz_daily_bars import BcolzDailyBarReader, BcolzDailyBarWriter
from zipline.data.bcolz_minute_bars import BcolzMinuteBarReader, BcolzMinuteBarWriter
from zipline.data.bundles.core import BundleData
from zipline.utils.calendar_utils import get_calendar

from .config import DEFAULT_BUNDLE_ROOT
from .exceptions import DataError
from .tushare_fields import FINANCIAL_FIELDS

BUNDLE_NAME = "tualpha"
BUNDLE_SCHEMA_VERSION = 5
NORMALIZED_STORE_SCHEMA_VERSION = 2
UPDATE_STATUS_SCHEMA_VERSION = 2
SID_MAP_VERSION = 1
VOLUME_MULTIPLIER = 100.0
_BUNDLE_NAME_PATTERN = re.compile(r"(?=.{1,64}\Z)[A-Za-z0-9]+(?:[._-][A-Za-z0-9]+)*\Z")
NORMALIZED_RAW_DIRS = (
    "daily",
    "fund_daily",
    "adj_factor",
    "fund_adj",
    "stk_limit",
    "suspend_d",
    "index_daily",
)
EXTENDED_DAILY_TABLES = {
    "daily_basic": frozenset(),
    "moneyflow": frozenset(),
    "industry": frozenset(
        {"l1_code", "l1_name", "l2_code", "l2_name", "l3_code", "l3_name"}
    ),
    "stock_st": frozenset({"name", "type", "type_name"}),
}
REQUIRED_RAW_DIRS = NORMALIZED_RAW_DIRS + tuple(EXTENDED_DAILY_TABLES)
FINANCIAL_TABLES = (
    "balancesheet",
    "income",
    "cashflow",
    "fina_indicator",
)
_IDENTIFIER_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_READ_LOCK_GUARD = Lock()
_READ_LOCKS: dict[str, tuple[FileLock, int]] = {}


@dataclass(frozen=True, slots=True)
class BundleAssetRecord:
    sid: int
    ts_code: str
    name: str
    asset_type: str
    exchange: str
    board: str
    price_tick: float
    start_date: pd.Timestamp
    end_date: pd.Timestamp


@dataclass(frozen=True, slots=True)
class BundleBuildResult:
    path: Path
    start_session: pd.Timestamp
    end_session: pd.Timestamp
    asset_count: int
    session_count: int


def validate_bundle_name(bundle_name: str) -> str:
    if not _BUNDLE_NAME_PATTERN.fullmatch(bundle_name):
        raise DataError(
            "bundle_name must contain only letters, digits, '.', '_' or '-', "
            "must not contain path separators, and must be at most 64 characters"
        )
    return bundle_name


def paths_overlap(first: str | Path, second: str | Path) -> bool:
    left = os.path.normcase(str(Path(first).expanduser().resolve()))
    right = os.path.normcase(str(Path(second).expanduser().resolve()))
    try:
        common = os.path.normcase(os.path.commonpath([left, right]))
    except ValueError:
        return False
    return common == left or common == right


def bundle_parent(bundle_root: str | Path = DEFAULT_BUNDLE_ROOT) -> Path:
    return Path(bundle_root).expanduser() / "bundles"


def bundle_path(
    bundle_root: str | Path = DEFAULT_BUNDLE_ROOT,
    bundle_name: str = BUNDLE_NAME,
) -> Path:
    return bundle_parent(bundle_root) / validate_bundle_name(bundle_name)


def cache_dir(
    bundle_root: str | Path = DEFAULT_BUNDLE_ROOT,
    bundle_name: str = BUNDLE_NAME,
) -> Path:
    return Path(bundle_root).expanduser() / "cache" / validate_bundle_name(bundle_name)


def normalized_store_path(
    bundle_root: str | Path = DEFAULT_BUNDLE_ROOT,
    bundle_name: str = BUNDLE_NAME,
) -> Path:
    return cache_dir(bundle_root, bundle_name) / "normalized.duckdb"


def bundle_lock_path(
    bundle_root: str | Path = DEFAULT_BUNDLE_ROOT,
    bundle_name: str = BUNDLE_NAME,
) -> Path:
    return (
        Path(bundle_root).expanduser()
        / ".locks"
        / f"{validate_bundle_name(bundle_name)}.bundle.lock"
    )


def acquire_bundle_read_lock(
    bundle_root: str | Path = DEFAULT_BUNDLE_ROOT,
    bundle_name: str = BUNDLE_NAME,
) -> tuple[str, FileLock]:
    path = bundle_lock_path(bundle_root, bundle_name)
    path.parent.mkdir(parents=True, exist_ok=True)
    key = os.path.normcase(str(path.resolve()))
    with _READ_LOCK_GUARD:
        existing = _READ_LOCKS.get(key)
        if existing is not None:
            lock, count = existing
            _READ_LOCKS[key] = (lock, count + 1)
            return key, lock
        lock = FileLock(str(path), thread_local=False)
        lock.acquire()
        _READ_LOCKS[key] = (lock, 1)
        return key, lock


def release_bundle_read_lock(key: str) -> None:
    with _READ_LOCK_GUARD:
        lock, count = _READ_LOCKS[key]
        if count > 1:
            _READ_LOCKS[key] = (lock, count - 1)
            return
        lock.release()
        del _READ_LOCKS[key]


def update_status_path(bundle_root: str | Path = DEFAULT_BUNDLE_ROOT) -> Path:
    return Path(bundle_root).expanduser() / "update-status.json"


def latest_bundle_path(
    bundle_root: str | Path = DEFAULT_BUNDLE_ROOT,
    bundle_name: str = BUNDLE_NAME,
) -> Path:
    path = bundle_path(bundle_root, bundle_name)
    if path.is_dir() and (path / "READY").is_file():
        return path
    candidates = sorted(
        path.parent.glob(f".previous-{path.name}-*"),
        key=lambda candidate: candidate.stat().st_mtime_ns,
        reverse=True,
    )
    for candidate in candidates:
        if candidate.is_dir() and (candidate / "READY").is_file():
            return candidate
    raise DataError(
        f"bundle does not exist or is incomplete; run `tualpha update`: {path}"
    )


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    os.replace(temporary, path)


def _record_bundle_build(
    root: Path,
    *,
    bundle_name: str,
    csv_dir: Path,
    started_at: str,
    result: BundleBuildResult,
) -> None:
    status_path = update_status_path(root)
    previous: dict[str, Any] = {}
    if status_path.is_file():
        try:
            previous = json.loads(status_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            previous = {}
    completed_at = datetime.now(ZoneInfo("UTC")).isoformat()
    build = {
        "started_at": started_at,
        "completed_at": completed_at,
        "bundle_path": str(result.path),
        "start_session": result.start_session.strftime("%Y-%m-%d"),
        "end_session": result.end_session.strftime("%Y-%m-%d"),
        "asset_count": result.asset_count,
        "session_count": result.session_count,
    }
    payload = {
        "schema_version": UPDATE_STATUS_SCHEMA_VERSION,
        "operation": "bundle_build",
        "status": "succeeded",
        "bundle_name": bundle_name,
        "csv_dir": str(csv_dir.resolve()),
        "bundle_root": str(root.resolve()),
        "last_bundle_build": build,
    }
    if "last_success" in previous:
        payload["last_success"] = previous["last_success"]
    _write_json_atomic(status_path, payload)


def _sql_path(path: Path) -> str:
    return path.resolve().as_posix().replace("'", "''")


def _csv_scan(path: Path) -> str:
    if not path.is_dir() or not any(path.glob("*.csv")):
        raise DataError(f"CSV source directory is empty or missing: {path}")
    glob = _sql_path(path / "*.csv")
    return (
        f"read_csv('{glob}', header=true, all_varchar=true, "
        "union_by_name=true, filename=false)"
    )


class NormalizedStore:
    """Columnar cache that transposes date-partitioned CSV data once."""

    def __init__(
        self,
        csv_dir: str | Path,
        bundle_root: str | Path = DEFAULT_BUNDLE_ROOT,
        bundle_name: str = BUNDLE_NAME,
    ) -> None:
        self.csv_dir = Path(csv_dir)
        self.bundle_root = Path(bundle_root).expanduser()
        self.bundle_name = bundle_name
        self.path = normalized_store_path(self.bundle_root, bundle_name)

    def _validate_sources(self) -> None:
        for dirname in REQUIRED_RAW_DIRS:
            path = self.csv_dir / dirname
            if not path.is_dir():
                raise DataError(f"required raw data directory does not exist: {path}")
        for filename in ("stock_basic.csv", "etf_basic.csv", "trade_cal.csv"):
            path = self.csv_dir / filename
            if not path.is_file():
                raise DataError(f"required raw data file does not exist: {path}")

    def _source_fingerprint(self) -> str:
        digest = hashlib.sha256()
        for directory in NORMALIZED_RAW_DIRS:
            for path in sorted((self.csv_dir / directory).glob("*.csv")):
                stat = path.stat()
                relative = path.relative_to(self.csv_dir).as_posix()
                digest.update(
                    f"{relative}\0{stat.st_size}\0{stat.st_mtime_ns}\n".encode()
                )
        return digest.hexdigest()

    def is_compatible(self, *, check_source: bool = True) -> bool:
        """Return whether the cache belongs to this CSV source and schema."""

        if not self.path.is_file():
            return False
        try:
            connection = duckdb.connect(str(self.path), read_only=True)
            try:
                metadata = dict(
                    connection.execute(
                        "SELECT key, value FROM store_metadata"
                    ).fetchall()
                )
            finally:
                connection.close()
        except (duckdb.Error, OSError):
            return False
        csv_value = metadata.get("csv_dir")
        if not csv_value:
            return False
        expected = os.path.normcase(str(self.csv_dir.resolve()))
        actual = os.path.normcase(str(Path(csv_value).resolve()))
        compatible = (
            metadata.get("schema_version") == str(NORMALIZED_STORE_SCHEMA_VERSION)
            and actual == expected
        )
        if not compatible or not check_source:
            return compatible
        return metadata.get("source_fingerprint") == self._source_fingerprint()

    def rebuild(self) -> Path:
        """Atomically rebuild normalized runtime tables from existing CSV files."""

        self._validate_sources()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_suffix(".building.duckdb")
        for candidate in (temporary, Path(f"{temporary}.wal")):
            candidate.unlink(missing_ok=True)
        connection = duckdb.connect(str(temporary))
        try:
            connection.execute("PRAGMA threads=4")
            self._create_price_tables(connection)
            self._create_metadata_tables(connection)
            self._create_index_table(connection)
            connection.execute(
                "CREATE TABLE store_metadata(key VARCHAR PRIMARY KEY, value VARCHAR)"
            )
            metadata = {
                "schema_version": str(NORMALIZED_STORE_SCHEMA_VERSION),
                "built_at": datetime.now(ZoneInfo("UTC")).isoformat(),
                "csv_dir": str(self.csv_dir.resolve()),
                "source_fingerprint": self._source_fingerprint(),
            }
            connection.executemany(
                "INSERT INTO store_metadata VALUES (?, ?)", metadata.items()
            )
            connection.execute("CHECKPOINT")
        except Exception:
            connection.close()
            temporary.unlink(missing_ok=True)
            raise
        else:
            connection.close()
        os.replace(temporary, self.path)
        return self.path

    def sync_dates(self, dates: Sequence[str | pd.Timestamp]) -> Path:
        """Incrementally replace changed daily partitions in the normalized cache."""

        if not self.is_compatible(check_source=False):
            return self.rebuild()
        normalized_dates = sorted(
            {pd.Timestamp(date).strftime("%Y%m%d") for date in dates}
        )
        if not normalized_dates:
            return self.path
        connection = duckdb.connect(str(self.path))
        try:
            connection.execute("BEGIN TRANSACTION")
            for date_key in normalized_dates:
                date_value = pd.to_datetime(date_key, format="%Y%m%d").date()
                scans = {}
                for table in REQUIRED_RAW_DIRS:
                    path = self.csv_dir / table / f"{date_key}.csv"
                    if not path.is_file():
                        raise DataError(
                            f"required updated partition does not exist: {path}"
                        )
                    scans[table] = (
                        f"read_csv('{_sql_path(path)}', header=true, "
                        "all_varchar=true, union_by_name=true)"
                    )

                connection.execute(
                    "DELETE FROM prices WHERE trade_date = ?", [date_value]
                )
                connection.execute(
                    f"""
                    INSERT INTO prices
                    SELECT upper(ts_code), strptime(trade_date, '%Y%m%d')::DATE,
                           try_cast(open AS DOUBLE), try_cast(high AS DOUBLE),
                           try_cast(low AS DOUBLE), try_cast(close AS DOUBLE),
                           try_cast(pre_close AS DOUBLE), try_cast(vol AS DOUBLE),
                           try_cast(amount AS DOUBLE) * 1000.0, 'stock'::VARCHAR
                    FROM {scans["daily"]}
                    UNION ALL
                    SELECT upper(ts_code), strptime(trade_date, '%Y%m%d')::DATE,
                           try_cast(open AS DOUBLE), try_cast(high AS DOUBLE),
                           try_cast(low AS DOUBLE), try_cast(close AS DOUBLE),
                           try_cast(pre_close AS DOUBLE), try_cast(vol AS DOUBLE),
                           try_cast(amount AS DOUBLE) * 1000.0, 'etf'::VARCHAR
                    FROM {scans["fund_daily"]}
                    """
                )
                connection.execute(
                    "DELETE FROM factors WHERE trade_date = ?", [date_value]
                )
                connection.execute(
                    f"""
                    INSERT INTO factors
                    SELECT upper(ts_code), strptime(trade_date, '%Y%m%d')::DATE,
                           try_cast(adj_factor AS DOUBLE), 'stock'::VARCHAR
                    FROM {scans["adj_factor"]}
                    UNION ALL
                    SELECT upper(ts_code), strptime(trade_date, '%Y%m%d')::DATE,
                           try_cast(adj_factor AS DOUBLE), 'etf'::VARCHAR
                    FROM {scans["fund_adj"]}
                    """
                )
                connection.execute(
                    "DELETE FROM limits WHERE trade_date = ?", [date_value]
                )
                connection.execute(
                    f"""
                    INSERT INTO limits
                    SELECT upper(ts_code), strptime(trade_date, '%Y%m%d')::DATE,
                           try_cast(up_limit AS DOUBLE), try_cast(down_limit AS DOUBLE)
                    FROM {scans["stk_limit"]}
                    """
                )
                connection.execute(
                    "DELETE FROM suspensions WHERE trade_date = ?", [date_value]
                )
                connection.execute(
                    f"""
                    INSERT INTO suspensions
                    SELECT upper(ts_code), strptime(trade_date, '%Y%m%d')::DATE,
                           max(CASE WHEN upper(suspend_type) = 'S' THEN 1 ELSE 0 END)::UTINYINT
                    FROM {scans["suspend_d"]}
                    GROUP BY ts_code, trade_date
                    """
                )
                connection.execute(
                    "DELETE FROM index_prices WHERE trade_date = ?", [date_value]
                )
                connection.execute(
                    f"""
                    INSERT INTO index_prices
                    SELECT upper(ts_code), strptime(trade_date, '%Y%m%d')::DATE,
                           try_cast(open AS DOUBLE), try_cast(high AS DOUBLE),
                           try_cast(low AS DOUBLE), try_cast(close AS DOUBLE),
                           try_cast(pre_close AS DOUBLE), try_cast(vol AS DOUBLE),
                           try_cast(amount AS DOUBLE) * 1000.0
                    FROM {scans["index_daily"]}
                    """
                )
            connection.execute(
                "INSERT OR REPLACE INTO store_metadata VALUES ('last_sync_at', ?)",
                [datetime.now(ZoneInfo("UTC")).isoformat()],
            )
            connection.execute(
                "INSERT OR REPLACE INTO store_metadata "
                "VALUES ('source_fingerprint', ?)",
                [self._source_fingerprint()],
            )
            connection.execute("COMMIT")
            connection.execute("CHECKPOINT")
        except Exception:
            connection.execute("ROLLBACK")
            raise
        finally:
            connection.close()
        return self.path

    def _create_price_tables(self, connection: duckdb.DuckDBPyConnection) -> None:
        daily = _csv_scan(self.csv_dir / "daily")
        fund_daily = _csv_scan(self.csv_dir / "fund_daily")
        connection.execute(
            f"""
            CREATE TABLE prices AS
            SELECT
                upper(ts_code) AS ts_code,
                strptime(trade_date, '%Y%m%d')::DATE AS trade_date,
                try_cast(open AS DOUBLE) AS open,
                try_cast(high AS DOUBLE) AS high,
                try_cast(low AS DOUBLE) AS low,
                try_cast(close AS DOUBLE) AS close,
                try_cast(pre_close AS DOUBLE) AS pre_close,
                try_cast(vol AS DOUBLE) AS volume,
                try_cast(amount AS DOUBLE) * 1000.0 AS turnover,
                'stock'::VARCHAR AS asset_type
            FROM {daily}
            UNION ALL
            SELECT
                upper(ts_code), strptime(trade_date, '%Y%m%d')::DATE,
                try_cast(open AS DOUBLE), try_cast(high AS DOUBLE),
                try_cast(low AS DOUBLE), try_cast(close AS DOUBLE),
                try_cast(pre_close AS DOUBLE), try_cast(vol AS DOUBLE),
                try_cast(amount AS DOUBLE) * 1000.0, 'etf'::VARCHAR
            FROM {fund_daily}
            """
        )
        connection.execute(
            "CREATE INDEX prices_code_date ON prices(ts_code, trade_date)"
        )

    def _create_metadata_tables(self, connection: duckdb.DuckDBPyConnection) -> None:
        adj_factor = _csv_scan(self.csv_dir / "adj_factor")
        fund_adj = _csv_scan(self.csv_dir / "fund_adj")
        limits = _csv_scan(self.csv_dir / "stk_limit")
        suspensions = _csv_scan(self.csv_dir / "suspend_d")
        connection.execute(
            f"""
            CREATE TABLE factors AS
            SELECT upper(ts_code) AS ts_code,
                   strptime(trade_date, '%Y%m%d')::DATE AS trade_date,
                   try_cast(adj_factor AS DOUBLE) AS adj_factor,
                   'stock'::VARCHAR AS asset_type
            FROM {adj_factor}
            UNION ALL
            SELECT upper(ts_code), strptime(trade_date, '%Y%m%d')::DATE,
                   try_cast(adj_factor AS DOUBLE), 'etf'::VARCHAR
            FROM {fund_adj}
            """
        )
        connection.execute(
            "CREATE INDEX factors_code_date ON factors(ts_code, trade_date)"
        )
        connection.execute(
            f"""
            CREATE TABLE limits AS
            SELECT upper(ts_code) AS ts_code,
                   strptime(trade_date, '%Y%m%d')::DATE AS trade_date,
                   try_cast(up_limit AS DOUBLE) AS up_limit,
                   try_cast(down_limit AS DOUBLE) AS down_limit
            FROM {limits}
            """
        )
        connection.execute(
            "CREATE INDEX limits_code_date ON limits(ts_code, trade_date)"
        )
        connection.execute(
            f"""
            CREATE TABLE suspensions AS
            SELECT upper(ts_code) AS ts_code,
                   strptime(trade_date, '%Y%m%d')::DATE AS trade_date,
                   max(CASE WHEN upper(suspend_type) = 'S' THEN 1 ELSE 0 END)::UTINYINT
                       AS suspended
            FROM {suspensions}
            GROUP BY ts_code, trade_date
            """
        )
        connection.execute(
            "CREATE INDEX suspensions_code_date ON suspensions(ts_code, trade_date)"
        )

    def _create_index_table(self, connection: duckdb.DuckDBPyConnection) -> None:
        index_daily = _csv_scan(self.csv_dir / "index_daily")
        connection.execute(
            f"""
            CREATE TABLE index_prices AS
            SELECT upper(ts_code) AS ts_code,
                   strptime(trade_date, '%Y%m%d')::DATE AS trade_date,
                   try_cast(open AS DOUBLE) AS open,
                   try_cast(high AS DOUBLE) AS high,
                   try_cast(low AS DOUBLE) AS low,
                   try_cast(close AS DOUBLE) AS close,
                   try_cast(pre_close AS DOUBLE) AS pre_close,
                   try_cast(vol AS DOUBLE) AS volume,
                   try_cast(amount AS DOUBLE) * 1000.0 AS turnover
            FROM {index_daily}
            """
        )
        connection.execute(
            "CREATE INDEX index_prices_code_date ON index_prices(ts_code, trade_date)"
        )

    def connect(self, *, read_only: bool = True) -> duckdb.DuckDBPyConnection:
        if not self.path.is_file():
            raise DataError(f"normalized store does not exist: {self.path}")
        return duckdb.connect(str(self.path), read_only=read_only)


class SidRegistry:
    """Persistent Tushare-code to Zipline sid mapping."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.mapping: dict[str, int] = {}
        if path.is_file():
            payload = json.loads(path.read_text(encoding="utf-8"))
            if payload.get("version") != SID_MAP_VERSION:
                raise DataError(f"unsupported sid map version in {path}")
            self.mapping = {
                str(code): int(sid) for code, sid in payload["assets"].items()
            }

    def assign(self, codes: Sequence[str]) -> dict[str, int]:
        next_sid = max(self.mapping.values(), default=0) + 1
        for code in sorted(set(codes)):
            if code not in self.mapping:
                self.mapping[code] = next_sid
                next_sid += 1
        return {code: self.mapping[code] for code in codes}

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_suffix(".tmp")
        payload = {"version": SID_MAP_VERSION, "assets": self.mapping}
        temporary.write_text(
            json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2),
            encoding="utf-8",
        )
        os.replace(temporary, self.path)


def _parse_date(value: object) -> pd.Timestamp | None:
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    return pd.to_datetime(text, format="%Y%m%d").normalize()


def _load_bundle_assets(
    csv_dir: Path,
    connection: duckdb.DuckDBPyConnection,
    sid_registry: SidRegistry,
    bundle_end: pd.Timestamp,
) -> list[BundleAssetRecord]:
    observed = connection.execute(
        """
        SELECT ts_code, asset_type, min(trade_date) AS first_date,
               max(trade_date) AS last_date
        FROM prices
        GROUP BY ts_code, asset_type
        """
    ).fetchdf()
    observed["ts_code"] = observed["ts_code"].astype(str).str.upper()
    observed_lookup = {
        (row.ts_code, row.asset_type): (
            pd.Timestamp(row.first_date),
            pd.Timestamp(row.last_date),
        )
        for row in observed.itertuples(index=False)
    }

    stock_frame = pd.read_csv(
        csv_dir / "stock_basic.csv", dtype=str, keep_default_na=False
    )
    etf_frame = pd.read_csv(csv_dir / "etf_basic.csv", dtype=str, keep_default_na=False)
    drafts: list[dict[str, Any]] = []
    board_map = {"主板": "main", "创业板": "chinext", "科创板": "star", "北交所": "bse"}
    exchange_map = {
        "SSE": "SSE",
        "SZSE": "SZSE",
        "BSE": "BSE",
        "SH": "SSE",
        "SZ": "SZSE",
    }

    for row in stock_frame.to_dict("records"):
        code = str(row.get("ts_code", "")).upper()
        dates = observed_lookup.get((code, "stock"))
        if not code or dates is None:
            continue
        first_observed, last_observed = dates
        list_date = _parse_date(row.get("list_date")) or first_observed
        delist_date = _parse_date(row.get("delist_date"))
        end_date = (
            min(bundle_end, delist_date) if delist_date is not None else bundle_end
        )
        end_date = max(first_observed, min(end_date, max(last_observed, end_date)))
        drafts.append(
            {
                "ts_code": code,
                "name": str(row.get("name", "")),
                "asset_type": "stock",
                "exchange": exchange_map.get(str(row.get("exchange", "")), "SSE"),
                "board": board_map.get(str(row.get("market", "")), "unknown"),
                "price_tick": 0.01,
                "start_date": max(list_date, first_observed),
                "end_date": end_date,
            }
        )

    for row in etf_frame.to_dict("records"):
        code = str(row.get("ts_code", "")).upper()
        dates = observed_lookup.get((code, "etf"))
        if not code or dates is None:
            continue
        first_observed, last_observed = dates
        list_date = _parse_date(row.get("list_date")) or first_observed
        end_date = (
            bundle_end if str(row.get("list_status", "")) == "L" else last_observed
        )
        drafts.append(
            {
                "ts_code": code,
                "name": str(row.get("extname") or row.get("csname") or ""),
                "asset_type": "etf",
                "exchange": exchange_map.get(str(row.get("exchange", "")), "SSE"),
                "board": "etf",
                "price_tick": 0.001,
                "start_date": max(list_date, first_observed),
                "end_date": min(bundle_end, end_date),
            }
        )

    sid_map = sid_registry.assign([str(draft["ts_code"]) for draft in drafts])
    return [
        BundleAssetRecord(sid=sid_map[str(draft["ts_code"])], **draft)
        for draft in drafts
        if draft["start_date"] <= draft["end_date"]
    ]


def _load_benchmark_assets(
    connection: duckdb.DuckDBPyConnection,
    sid_registry: SidRegistry,
    bundle_start: pd.Timestamp,
    bundle_end: pd.Timestamp,
) -> list[BundleAssetRecord]:
    frame = connection.execute(
        """
        SELECT ts_code, min(trade_date) AS first_date,
               max(trade_date) AS last_date
        FROM index_prices
        GROUP BY ts_code
        ORDER BY ts_code
        """
    ).fetchdf()
    if frame.empty:
        return []
    codes = frame["ts_code"].astype(str).str.upper().tolist()
    sid_map = sid_registry.assign(codes)
    records = []
    for row in frame.itertuples(index=False):
        code = str(row.ts_code).upper()
        start_date = max(bundle_start, pd.Timestamp(row.first_date))
        end_date = min(bundle_end, pd.Timestamp(row.last_date))
        if start_date > end_date:
            continue
        suffix = code.rsplit(".", 1)[-1] if "." in code else "INDEX"
        records.append(
            BundleAssetRecord(
                sid=sid_map[code],
                ts_code=code,
                name=code,
                asset_type="index",
                exchange=suffix,
                board="index",
                price_tick=0.01,
                start_date=start_date,
                end_date=end_date,
            )
        )
    return records


def _asset_frame(records: Sequence[BundleAssetRecord], calendar: Any) -> pd.DataFrame:
    rows = []
    for record in records:
        try:
            auto_close = calendar.next_session(record.end_date)
        except (IndexError, ValueError):
            auto_close = record.end_date + pd.Timedelta(days=1)
        rows.append(
            {
                "sid": record.sid,
                "symbol": record.ts_code,
                "asset_name": record.name,
                "start_date": record.start_date,
                "end_date": record.end_date,
                "first_traded": record.start_date,
                "auto_close_date": auto_close,
                "exchange": record.exchange,
            }
        )
    return pd.DataFrame(rows).set_index("sid").sort_index()


def _asset_supplementary_frame(
    records: Sequence[BundleAssetRecord],
) -> pd.DataFrame:
    rows = []
    for record in records:
        for field, value in (
            ("asset_type", record.asset_type),
            ("board", record.board),
            ("price_tick", str(record.price_tick)),
        ):
            rows.append(
                {
                    "sid": record.sid,
                    "field": field,
                    "start_date": record.start_date.value,
                    "end_date": record.end_date.value,
                    "value": value,
                }
            )
    return pd.DataFrame(rows)


def _daily_data(
    connection: duckdb.DuckDBPyConnection,
    records: Sequence[BundleAssetRecord],
    calendar: Any,
) -> Iterator[tuple[int, pd.DataFrame]]:
    for record in sorted(records, key=lambda item: item.sid):
        if record.asset_type == "index":
            query = """
                SELECT trade_date, open, high, low, close, volume
                FROM index_prices
                WHERE ts_code = ? AND trade_date BETWEEN ? AND ?
                ORDER BY trade_date
            """
            parameters = [
                record.ts_code,
                record.start_date.date(),
                record.end_date.date(),
            ]
        else:
            query = """
                SELECT trade_date, open, high, low, close, volume
                FROM prices
                WHERE ts_code = ? AND asset_type = ?
                  AND trade_date BETWEEN ? AND ?
                ORDER BY trade_date
            """
            parameters = [
                record.ts_code,
                record.asset_type,
                record.start_date.date(),
                record.end_date.date(),
            ]
        frame = connection.execute(query, parameters).fetchdf()
        if pd.api.types.is_datetime64_any_dtype(frame["trade_date"]):
            frame["trade_date"] = frame["trade_date"].astype("datetime64[ns]")
        else:
            frame["trade_date"] = pd.to_datetime(frame["trade_date"])
        frame = frame.drop_duplicates("trade_date", keep="last").set_index("trade_date")
        sessions = calendar.sessions_in_range(record.start_date, record.end_date)
        frame = frame.reindex(sessions)
        for column in ("open", "high", "low", "close", "volume"):
            frame[column] = pd.to_numeric(frame[column], errors="coerce").fillna(0.0)
        # Official daily Bcolz stores uint32 share volume. A few highly liquid
        # ETFs exceed that hard limit; the official column is capped while
        # TuAlpha reads the exact share volume from an appended Bcolz column.
        frame["volume"] = np.minimum(
            np.rint(frame["volume"] * VOLUME_MULTIPLIER),
            np.iinfo(np.uint32).max - 1,
        )
        yield record.sid, frame[["open", "high", "low", "close", "volume"]]


def _write_index_bcolz(
    normalized_path: Path,
    index_path: Path,
    records: Sequence[BundleAssetRecord],
) -> None:
    names = (
        "open",
        "high",
        "low",
        "close",
        "pre_close",
        "volume",
        "turnover",
        "day",
        "id",
    )
    dtypes = ("f8", "f8", "f8", "f8", "f8", "f8", "f8", "u4", "u4")
    table = bcolz.ctable(
        columns=[np.array([], dtype=dtype) for dtype in dtypes],
        names=names,
        rootdir=str(index_path),
        mode="w",
    )
    first_row: dict[str, int] = {}
    last_row: dict[str, int] = {}
    if records:
        mapping = pd.DataFrame(
            [
                (
                    record.ts_code,
                    record.sid,
                    record.start_date.date(),
                    record.end_date.date(),
                )
                for record in records
            ],
            columns=["ts_code", "sid", "start_date", "end_date"],
        )
        connection = duckdb.connect(str(normalized_path), read_only=True)
        connection.register("benchmark_assets", mapping)
        try:
            cursor = connection.execute(
                """
                SELECT a.sid, epoch(i.trade_date)::UBIGINT AS day,
                       i.open, i.high, i.low, i.close, i.pre_close,
                       i.volume, i.turnover
                FROM index_prices i
                JOIN benchmark_assets a USING (ts_code)
                WHERE i.trade_date BETWEEN a.start_date AND a.end_date
                ORDER BY a.sid, i.trade_date
                """
            )
            total_rows = 0
            while True:
                frame = cursor.fetch_df_chunk(64)
                if frame.empty:
                    break
                ids = pd.to_numeric(frame["sid"], errors="raise").to_numpy(
                    dtype=np.uint32
                )
                unique, positions, counts = np.unique(
                    ids, return_index=True, return_counts=True
                )
                for sid, position, count in zip(unique, positions, counts, strict=True):
                    key = str(int(sid))
                    first_row.setdefault(key, total_rows + int(position))
                    last_row[key] = total_rows + int(position + count - 1)
                arrays = [
                    pd.to_numeric(frame[name], errors="coerce").to_numpy(
                        dtype=np.float64
                    )
                    for name in names[:7]
                ]
                arrays.extend(
                    [
                        pd.to_numeric(frame["day"], errors="raise").to_numpy(
                            dtype=np.uint32
                        ),
                        ids,
                    ]
                )
                table.append(arrays)
                total_rows += len(frame)
        finally:
            connection.unregister("benchmark_assets")
            connection.close()
    table.attrs["first_row"] = first_row
    table.attrs["last_row"] = last_row
    table.attrs["tualpha_schema_version"] = BUNDLE_SCHEMA_VERSION
    table.flush()


def _adjustment_events(
    connection: duckdb.DuckDBPyConnection,
    records: Sequence[BundleAssetRecord],
) -> pd.DataFrame:
    mapping = pd.DataFrame(
        [(record.ts_code, record.asset_type, record.sid) for record in records],
        columns=["ts_code", "asset_type", "sid"],
    )
    connection.register("bundle_sid_map", mapping)
    try:
        events = connection.execute(
            """
            WITH ordered AS (
                SELECT m.sid, f.trade_date, f.adj_factor,
                       lag(f.adj_factor) OVER (
                           PARTITION BY m.sid ORDER BY f.trade_date
                       ) AS previous_factor
                FROM factors f
                JOIN bundle_sid_map m USING (ts_code, asset_type)
                WHERE f.adj_factor IS NOT NULL AND f.adj_factor > 0
            )
            SELECT sid, trade_date AS effective_date,
                   previous_factor / adj_factor AS ratio
            FROM ordered
            WHERE previous_factor IS NOT NULL
              AND abs(previous_factor - adj_factor) > 1e-12
            ORDER BY sid, effective_date
            """
        ).fetchdf()
    finally:
        connection.unregister("bundle_sid_map")
    if events.empty:
        return pd.DataFrame(columns=["sid", "effective_date", "ratio"])
    events["sid"] = events["sid"].astype(np.int64)
    events["effective_date"] = pd.to_datetime(events["effective_date"])
    events["ratio"] = events["ratio"].astype(float)
    return events


def _quote_identifier(name: str) -> str:
    if not _IDENTIFIER_PATTERN.fullmatch(name):
        raise DataError(f"unsafe CSV column name: {name!r}")
    return f'"{name}"'


def _scan_columns(connection: duckdb.DuckDBPyConnection, scan: str) -> list[str]:
    columns = [
        str(row[0])
        for row in connection.execute(f"DESCRIBE SELECT * FROM {scan}").fetchall()
    ]
    for column in columns:
        _quote_identifier(column)
    return columns


def _optional_csv_scan(path: Path) -> str | None:
    if not path.is_dir() or not any(path.glob("*.csv")):
        return None
    return _csv_scan(path)


def _string_expression(column: str) -> str:
    quoted = _quote_identifier(column)
    return f"nullif(trim({quoted}), '')::VARCHAR AS {quoted}"


def _numeric_expression(column: str) -> str:
    quoted = _quote_identifier(column)
    return f"try_cast(nullif({quoted}, '') AS DOUBLE) AS {quoted}"


def _date_expression(column: str) -> str:
    quoted = _quote_identifier(column)
    return f"try_strptime(nullif({quoted}, ''), '%Y%m%d')::DATE AS {quoted}"


def _create_extended_daily_table(
    connection: duckdb.DuckDBPyConnection,
    csv_dir: Path,
    table: str,
    string_fields: frozenset[str],
    *,
    catalog: str,
) -> None:
    scan = _csv_scan(csv_dir / table)
    columns = _scan_columns(connection, scan)
    required = {"ts_code", "trade_date"}
    if not required.issubset(columns):
        raise DataError(
            f"{table} CSV is missing columns: {sorted(required.difference(columns))}"
        )
    value_columns = [
        column for column in columns if column not in {"ts_code", "trade_date"}
    ]
    expressions = [
        "upper(nullif(trim(ts_code), ''))::VARCHAR AS ts_code",
        _date_expression("trade_date"),
        *[
            _string_expression(column)
            if column in string_fields
            else _numeric_expression(column)
            for column in value_columns
        ],
    ]
    if table == "stock_st":
        expressions.append("1::UTINYINT AS is_st")
        value_columns.append("is_st")
    selected = ", ".join(f"t.{_quote_identifier(column)}" for column in value_columns)
    selected = f", {selected}" if selected else ""
    quoted_table = _quote_identifier(table)
    connection.execute(
        f"""
        CREATE TABLE {_quote_identifier(catalog)}.{quoted_table} AS
        WITH typed AS (
            SELECT {", ".join(expressions)} FROM {scan}
        ), ranked AS (
            SELECT *, row_number() OVER (
                PARTITION BY ts_code, trade_date ORDER BY ts_code
            ) AS _rank
            FROM typed
            WHERE ts_code IS NOT NULL AND trade_date IS NOT NULL
        )
        SELECT a.sid, epoch(t.trade_date)::BIGINT AS session{selected}
        FROM ranked t
        JOIN bundle_assets a USING (ts_code)
        WHERE t._rank = 1
        """
    )
    connection.execute(
        f"CREATE UNIQUE INDEX {_quote_identifier(f'{table}_sid_session')} "
        f"ON {_quote_identifier(catalog)}.{quoted_table}(sid, session)"
    )


def _create_empty_financial_table(
    connection: duckdb.DuckDBPyConnection, table: str, *, catalog: str
) -> None:
    metadata = {
        "ts_code",
        "ann_date",
        "f_ann_date",
        "end_date",
        "report_type",
        "comp_type",
        "end_type",
        "update_flag",
    }
    values = ", ".join(
        f"{_quote_identifier(column)} DOUBLE"
        for column in FINANCIAL_FIELDS[table]
        if column not in metadata
    )
    values = f", {values}" if values else ""
    connection.execute(
        f"""
        CREATE TABLE {_quote_identifier(catalog)}.{_quote_identifier(f"financial_{table}")} (
            sid BIGINT, ann_date DATE, f_ann_date DATE,
            effective_ann_date DATE, end_date DATE,
            report_type VARCHAR, comp_type VARCHAR, end_type VARCHAR,
            update_flag VARCHAR, source_order BIGINT{values}
        )
        """
    )


def _create_financial_table(
    connection: duckdb.DuckDBPyConnection,
    csv_dir: Path,
    table: str,
    *,
    catalog: str,
) -> None:
    scan = _optional_csv_scan(csv_dir / table)
    if scan is None:
        _create_empty_financial_table(connection, table, catalog=catalog)
        return
    columns = _scan_columns(connection, scan)
    if not {"ts_code", "ann_date", "end_date"}.issubset(columns):
        raise DataError(f"{table} CSV is missing financial key columns")
    metadata_columns = {
        "ts_code",
        "ann_date",
        "f_ann_date",
        "end_date",
        "report_type",
        "comp_type",
        "end_type",
        "update_flag",
    }
    documented_values = [
        column for column in FINANCIAL_FIELDS[table] if column not in metadata_columns
    ]
    value_columns = list(
        dict.fromkeys(
            [
                *documented_values,
                *(column for column in columns if column not in metadata_columns),
            ]
        )
    )

    def optional_expression(column: str, kind: str) -> str:
        if column not in columns:
            sql_type = "DATE" if kind == "date" else "VARCHAR"
            return f"NULL::{sql_type} AS {_quote_identifier(column)}"
        if kind == "date":
            return _date_expression(column)
        return _string_expression(column)

    expressions = [
        "upper(nullif(trim(ts_code), ''))::VARCHAR AS ts_code",
        _date_expression("ann_date"),
        optional_expression("f_ann_date", "date"),
        _date_expression("end_date"),
        optional_expression("report_type", "string"),
        optional_expression("comp_type", "string"),
        optional_expression("end_type", "string"),
        optional_expression("update_flag", "string"),
        "row_number() OVER ()::BIGINT AS source_order",
        *[
            _numeric_expression(column)
            if column in columns
            else f"NULL::DOUBLE AS {_quote_identifier(column)}"
            for column in value_columns
        ],
    ]
    selected_values = ", ".join(
        f"t.{_quote_identifier(column)}" for column in value_columns
    )
    selected_values = f", {selected_values}" if selected_values else ""
    financial_table = _quote_identifier(f"financial_{table}")
    connection.execute(
        f"""
        CREATE TABLE {_quote_identifier(catalog)}.{financial_table} AS
        WITH raw_typed AS (
            SELECT {", ".join(expressions)} FROM {scan}
        ), typed AS (
            SELECT *, coalesce(f_ann_date, ann_date) AS effective_ann_date
            FROM raw_typed
        )
        SELECT a.sid, t.ann_date, t.f_ann_date, t.effective_ann_date,
               t.end_date, t.report_type, t.comp_type, t.end_type,
               t.update_flag, t.source_order{selected_values}
        FROM typed t
        JOIN bundle_assets a USING (ts_code)
        WHERE t.end_date IS NOT NULL AND t.effective_ann_date IS NOT NULL
        """
    )


def _prepare_daily_extension_tables(
    connection: duckdb.DuckDBPyConnection,
    csv_dir: Path,
    records: Sequence[BundleAssetRecord],
    catalog: str,
) -> None:
    mapping = pd.DataFrame([asdict(record) for record in records])
    mapping["start_date"] = pd.to_datetime(mapping["start_date"])
    mapping["end_date"] = pd.to_datetime(mapping["end_date"])
    connection.register("bundle_assets", mapping)
    quoted_catalog = _quote_identifier(catalog)
    connection.execute(
        f"""
        CREATE TABLE {quoted_catalog}.daily_metadata AS
        WITH keys AS (
            SELECT p.ts_code, p.asset_type, p.trade_date FROM prices p
            JOIN bundle_assets a USING (ts_code, asset_type)
            UNION
            SELECT f.ts_code, f.asset_type, f.trade_date FROM factors f
            JOIN bundle_assets a USING (ts_code, asset_type)
            UNION
            SELECT l.ts_code, a.asset_type, l.trade_date FROM limits l
            JOIN bundle_assets a USING (ts_code)
            WHERE a.asset_type != 'index'
            UNION
            SELECT s.ts_code, a.asset_type, s.trade_date FROM suspensions s
            JOIN bundle_assets a USING (ts_code)
            WHERE a.asset_type != 'index'
        ),
        p AS (
            SELECT ts_code, asset_type, trade_date,
                   max(pre_close) AS pre_close,
                   max(turnover) AS turnover,
                   max(volume) AS volume
            FROM prices GROUP BY ALL
        ),
        f AS (
            SELECT ts_code, asset_type, trade_date,
                   max(adj_factor) AS adj_factor
            FROM factors GROUP BY ALL
        ),
        l AS (
            SELECT ts_code, trade_date, max(up_limit) AS up_limit,
                   max(down_limit) AS down_limit
            FROM limits GROUP BY ALL
        )
        SELECT a.sid, epoch(k.trade_date)::BIGINT AS session,
               p.pre_close, p.turnover, p.volume,
               l.up_limit, l.down_limit, f.adj_factor,
               coalesce(s.suspended, 0)::UTINYINT AS suspended
        FROM keys k
        JOIN bundle_assets a USING (ts_code, asset_type)
        LEFT JOIN p USING (ts_code, asset_type, trade_date)
        LEFT JOIN f USING (ts_code, asset_type, trade_date)
        LEFT JOIN l USING (ts_code, trade_date)
        LEFT JOIN suspensions s USING (ts_code, trade_date)
        """
    )
    connection.execute(
        f"CREATE UNIQUE INDEX daily_sid_session "
        f"ON {quoted_catalog}.daily_metadata(sid, session)"
    )
    for table, string_fields in EXTENDED_DAILY_TABLES.items():
        _create_extended_daily_table(
            connection,
            csv_dir,
            table,
            string_fields,
            catalog=catalog,
        )


def _daily_extension_registry(
    connection: duckdb.DuckDBPyConnection, catalog: str
) -> dict[str, dict[str, Any]]:
    registry: dict[str, dict[str, Any]] = {
        "pre_close": {"column": "ta_pre_close", "kind": "numeric"},
        "turnover": {"column": "ta_turnover", "kind": "numeric"},
        "volume": {"column": "ta_volume_exact", "kind": "numeric"},
        "up_limit": {"column": "ta_up_limit", "kind": "numeric"},
        "down_limit": {"column": "ta_down_limit", "kind": "numeric"},
        "adj_factor": {"column": "ta_adj_factor", "kind": "numeric"},
        "suspended": {"column": "ta_suspended", "kind": "flag"},
    }
    for table, string_fields in EXTENDED_DAILY_TABLES.items():
        rows = connection.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_catalog = ? AND table_name = ?
            ORDER BY ordinal_position
            """,
            [catalog, table],
        ).fetchall()
        for (column,) in rows:
            name = str(column)
            if name in {"sid", "session"}:
                continue
            public = f"{table}.{name}"
            registry[public] = {
                "column": f"ta_{table}__{name}",
                "kind": "categorical" if name in string_fields else "numeric",
            }
    registry["stock_st.is_st"]["kind"] = "flag"
    return registry


def _append_daily_extensions(
    normalized_path: Path,
    csv_dir: Path,
    daily_path: Path,
    output_dir: Path,
    records: Sequence[BundleAssetRecord],
) -> dict[str, dict[str, Any]]:
    temporary_db = output_dir / ".daily-extensions.duckdb"
    column_root = output_dir / ".daily-columns"
    temporary_db.unlink(missing_ok=True)
    shutil.rmtree(column_root, ignore_errors=True)
    column_root.mkdir()
    connection = duckdb.connect(str(normalized_path), read_only=False)
    catalog = "daily_extensions"
    connection.execute("PRAGMA threads=4")
    connection.execute(
        f"ATTACH '{_sql_path(temporary_db)}' AS {_quote_identifier(catalog)}"
    )
    table = bcolz.open(rootdir=str(daily_path), mode="a")
    columns: dict[str, Any] = {}
    try:
        _prepare_daily_extension_tables(connection, csv_dir, records, catalog)
        registry = _daily_extension_registry(connection, catalog)
        for spec in registry.values():
            physical = spec["column"]
            kind = spec["kind"]
            dtype = "i4" if kind == "categorical" else "u1" if kind == "flag" else "f8"
            columns[physical] = bcolz.carray(
                np.array([], dtype=dtype),
                rootdir=str(column_root / physical),
                mode="w",
            )

        aliases = {
            table_name: f"e{index}"
            for index, table_name in enumerate(EXTENDED_DAILY_TABLES)
        }
        selected = [
            'm.pre_close AS "ta_pre_close"',
            'm.turnover AS "ta_turnover"',
            f'm.volume * {VOLUME_MULTIPLIER} AS "ta_volume_exact"',
            'm.up_limit AS "ta_up_limit"',
            'm.down_limit AS "ta_down_limit"',
            'm.adj_factor AS "ta_adj_factor"',
            'coalesce(m.suspended, 0) AS "ta_suspended"',
        ]
        category_joins = []
        category_index = 0
        for table_name, alias in aliases.items():
            for public, spec in registry.items():
                prefix = f"{table_name}."
                if not public.startswith(prefix):
                    continue
                column = public[len(prefix) :]
                expression = f"{alias}.{_quote_identifier(column)}"
                if spec["kind"] == "categorical":
                    category_table = f"category_{category_index}"
                    category_alias = f"c{category_index}"
                    category_index += 1
                    connection.execute(
                        f"""
                        CREATE TABLE {_quote_identifier(catalog)}.{_quote_identifier(category_table)} AS
                        SELECT (row_number() OVER (ORDER BY value) - 1)::INTEGER AS code,
                               value
                        FROM (
                            SELECT DISTINCT {_quote_identifier(column)} AS value
                            FROM {_quote_identifier(catalog)}.{_quote_identifier(table_name)}
                            WHERE {_quote_identifier(column)} IS NOT NULL
                        ) category_values
                        """
                    )
                    categories = connection.execute(
                        f"SELECT value FROM {_quote_identifier(catalog)}."
                        f"{_quote_identifier(category_table)} ORDER BY code"
                    ).fetchall()
                    spec["categories"] = [str(row[0]) for row in categories]
                    category_joins.append(
                        f"LEFT JOIN {_quote_identifier(catalog)}."
                        f"{_quote_identifier(category_table)} {category_alias} ON "
                        f"{category_alias}.value = {expression}"
                    )
                    expression = f"coalesce({category_alias}.code, -1)"
                elif public == "stock_st.is_st":
                    expression = f"coalesce({expression}, 0)"
                selected.append(f"{expression} AS {_quote_identifier(spec['column'])}")
        joins = "\n".join(
            [
                *(
                    f"LEFT JOIN {_quote_identifier(catalog)}."
                    f"{_quote_identifier(table_name)} {alias} ON "
                    f"{alias}.sid = k.sid AND {alias}.session = k.session"
                    for table_name, alias in aliases.items()
                ),
                *category_joins,
            ]
        )
        total_rows = len(table)
        connection.execute(
            f"CREATE TABLE {_quote_identifier(catalog)}.row_keys("
            "row_number BIGINT, sid BIGINT, session BIGINT)"
        )
        key_batch_size = 500_000
        for start in range(0, total_rows, key_batch_size):
            stop = min(start + key_batch_size, total_rows)
            keys = pd.DataFrame(
                {
                    "row_number": np.arange(start, stop, dtype=np.int64),
                    "sid": np.asarray(table["id"][start:stop], dtype=np.int64),
                    "session": np.asarray(table["day"][start:stop], dtype=np.int64),
                }
            )
            connection.register("daily_batch_keys", keys)
            try:
                connection.execute(
                    f"INSERT INTO {_quote_identifier(catalog)}.row_keys "
                    "SELECT row_number, sid, session FROM daily_batch_keys"
                )
            finally:
                connection.unregister("daily_batch_keys")
        connection.execute(
            f"CREATE UNIQUE INDEX row_keys_sid_session ON "
            f"{_quote_identifier(catalog)}.row_keys(sid, session)"
        )

        cursor = connection.execute(
            f"""
            SELECT {", ".join(selected)}
            FROM {_quote_identifier(catalog)}.row_keys k
            LEFT JOIN {_quote_identifier(catalog)}.daily_metadata m
              ON m.sid = k.sid AND m.session = k.session
            {joins}
            ORDER BY k.row_number
            """
        )
        received = 0
        while True:
            frame = cursor.fetch_df_chunk(64)
            if frame.empty:
                break
            received += len(frame)
            for spec in registry.values():
                physical = spec["column"]
                kind = spec["kind"]
                if kind == "categorical":
                    values = (
                        pd.to_numeric(frame[physical], errors="coerce")
                        .fillna(-1)
                        .to_numpy(dtype=np.int32)
                    )
                elif kind == "flag":
                    values = (
                        pd.to_numeric(frame[physical], errors="coerce")
                        .fillna(0)
                        .to_numpy(dtype=np.uint8)
                    )
                else:
                    values = pd.to_numeric(frame[physical], errors="coerce").to_numpy(
                        dtype=np.float64
                    )
                columns[physical].append(values)
        if received != total_rows:
            raise DataError("daily extension join changed Bcolz row cardinality")

        for spec in registry.values():
            physical = spec["column"]
            column = columns.pop(physical)
            if len(column) != len(table):
                raise DataError(f"Bcolz extension column has wrong length: {physical}")
            column.flush()
            source = Path(column.rootdir)
            destination = daily_path / physical
            os.replace(source, destination)
            moved = bcolz.open(rootdir=str(destination), mode="a")
            table.cols.insert(physical, len(table.names), moved)
            table._arr1 = np.empty(shape=(1,), dtype=table.dtype)
        table.attrs["tualpha_schema_version"] = BUNDLE_SCHEMA_VERSION
        table.attrs["tualpha_fields"] = registry
        table.flush()
        return registry
    finally:
        try:
            connection.execute(f"DETACH {_quote_identifier(catalog)}")
        finally:
            try:
                connection.unregister("bundle_assets")
            except duckdb.Error:
                pass
            connection.close()
        temporary_db.unlink(missing_ok=True)
        Path(f"{temporary_db}.wal").unlink(missing_ok=True)
        shutil.rmtree(column_root, ignore_errors=True)


def _write_finance(
    normalized_path: Path,
    csv_dir: Path,
    output_dir: Path,
    records: Sequence[BundleAssetRecord],
) -> None:
    finance_path = output_dir / "finance.sqlite"
    finance_path.unlink(missing_ok=True)
    connection = duckdb.connect(str(normalized_path), read_only=False)
    mapping = pd.DataFrame([asdict(record) for record in records])
    connection.register("bundle_assets", mapping)
    try:
        connection.execute(
            f"ATTACH '{_sql_path(finance_path)}' AS finance (TYPE SQLITE)"
        )
        for table in FINANCIAL_TABLES:
            _create_financial_table(connection, csv_dir, table, catalog="finance")
        connection.execute(
            "CREATE TABLE finance.finance_metadata(key VARCHAR PRIMARY KEY, value VARCHAR)"
        )
        connection.executemany(
            "INSERT INTO finance.finance_metadata VALUES (?, ?)",
            {
                "schema_version": str(BUNDLE_SCHEMA_VERSION),
                "point_in_time_rule": "effective_ann_date < session",
            }.items(),
        )
        connection.execute("DETACH finance")
    finally:
        connection.unregister("bundle_assets")
        connection.close()

    sqlite = sqlite3.connect(finance_path)
    try:
        for table in FINANCIAL_TABLES:
            sqlite.execute(
                f"CREATE INDEX {_quote_identifier(f'financial_{table}_point_in_time')} "
                f"ON {_quote_identifier(f'financial_{table}')}"
                "(sid, effective_ann_date, end_date, report_type, update_flag, source_order)"
            )
        sqlite.commit()
        result = sqlite.execute("PRAGMA integrity_check").fetchone()
        if result != ("ok",):
            raise DataError(f"finance.sqlite integrity check failed: {result}")
    finally:
        sqlite.close()


def _write_index_constituents(
    csv_dir: Path,
    output_dir: Path,
    records: Sequence[BundleAssetRecord],
    end_session: pd.Timestamp,
    generated_at: str,
) -> dict[str, Any]:
    destination = output_dir / "index_constituents.sqlite"
    destination.unlink(missing_ok=True)
    connection = sqlite3.connect(destination)
    try:
        connection.execute("PRAGMA journal_mode = OFF")
        connection.execute("PRAGMA synchronous = OFF")
        connection.execute(
            """
            CREATE TABLE index_constituents(
                index_code TEXT NOT NULL,
                snapshot_date TEXT NOT NULL,
                con_code TEXT NOT NULL,
                sid INTEGER,
                weight REAL NOT NULL,
                PRIMARY KEY(index_code, snapshot_date, con_code)
            ) WITHOUT ROWID
            """
        )
        sid_by_code = {record.ts_code: record.sid for record in records}
        maximum_date = end_session.strftime("%Y%m%d")
        source = csv_dir / "index_weight"
        for path in sorted(source.glob("*.csv")) if source.is_dir() else ():
            try:
                frame = pd.read_csv(path, dtype=str, keep_default_na=False)
            except pd.errors.EmptyDataError:
                continue
            columns = ("index_code", "con_code", "trade_date", "weight")
            missing = set(columns).difference(frame.columns)
            if missing:
                raise DataError(
                    f"index weight CSV is missing columns {sorted(missing)}: {path}"
                )
            frame = frame[list(columns)].copy()
            frame["index_code"] = frame["index_code"].str.upper()
            frame["con_code"] = frame["con_code"].str.upper()
            frame["trade_date"] = frame["trade_date"].astype(str)
            frame = frame[frame["trade_date"] <= maximum_date]
            if frame.empty:
                continue
            dates = pd.to_datetime(
                frame["trade_date"], format="%Y%m%d", errors="coerce"
            )
            weights = pd.to_numeric(frame["weight"], errors="coerce")
            invalid = (
                dates.isna()
                | weights.isna()
                | ~weights.between(0.0, 100.0)
                | frame["index_code"].eq("")
                | frame["con_code"].eq("")
            )
            if invalid.any():
                raise DataError(f"index weight CSV contains invalid rows: {path}")
            rows = [
                (
                    str(index_code),
                    snapshot_date.strftime("%Y-%m-%d"),
                    str(con_code),
                    sid_by_code.get(str(con_code)),
                    float(weight),
                )
                for index_code, con_code, snapshot_date, weight in zip(
                    frame["index_code"],
                    frame["con_code"],
                    dates,
                    weights,
                    strict=True,
                )
            ]
            connection.executemany(
                "INSERT OR REPLACE INTO index_constituents VALUES (?, ?, ?, ?, ?)",
                rows,
            )
        connection.execute(
            "CREATE INDEX index_constituents_snapshot "
            "ON index_constituents(index_code, snapshot_date)"
        )
        connection.execute(
            "CREATE INDEX index_constituents_member "
            "ON index_constituents(con_code, snapshot_date)"
        )
        connection.execute(
            "CREATE TABLE index_constituent_metadata"
            "(key TEXT PRIMARY KEY, value TEXT NOT NULL)"
        )
        connection.executemany(
            "INSERT INTO index_constituent_metadata VALUES (?, ?)",
            {
                "schema_version": str(BUNDLE_SCHEMA_VERSION),
                "generated_at": generated_at,
                "point_in_time_rule": "snapshot_date < session",
                "weight_unit": "percent",
            }.items(),
        )
        connection.commit()
        integrity = connection.execute("PRAGMA integrity_check").fetchone()
        if integrity != ("ok",):
            raise DataError(
                f"index_constituents.sqlite integrity check failed: {integrity}"
            )
        row = connection.execute(
            """
            SELECT count(*), count(DISTINCT index_code || ':' || snapshot_date),
                   min(snapshot_date), max(snapshot_date)
            FROM index_constituents
            """
        ).fetchone()
        codes = [
            str(item[0])
            for item in connection.execute(
                "SELECT DISTINCT index_code FROM index_constituents ORDER BY index_code"
            )
        ]
    finally:
        connection.close()
    return {
        "codes": codes,
        "rows": int(row[0]),
        "snapshots": int(row[1]),
        "start": row[2],
        "end": row[3],
    }


def _write_manifest(
    output_dir: Path,
    records: Sequence[BundleAssetRecord],
    benchmark_records: Sequence[BundleAssetRecord],
    start_session: pd.Timestamp,
    end_session: pd.Timestamp,
    bundle_name: str,
    generated_at: str,
    index_constituents: dict[str, Any],
) -> None:
    manifest = {
        "schema_version": BUNDLE_SCHEMA_VERSION,
        "bundle_name": bundle_name,
        "layout": "fixed",
        "generated_at": generated_at,
        "start_session": start_session.strftime("%Y-%m-%d"),
        "end_session": end_session.strftime("%Y-%m-%d"),
        "asset_count": len(records),
        "volume_unit": "shares",
        "volume_overflow": "official_bcolz_capped; exact value in ta_volume_exact",
        "volume_multiplier": VOLUME_MULTIPLIER,
        "settlement_days": 1,
        "daily_extensions": "daily_equities.bcolz",
        "index_prices": "index_daily.bcolz",
        "index_constituents": "index_constituents.sqlite",
        "index_constituent_codes": index_constituents["codes"],
        "index_constituent_rows": index_constituents["rows"],
        "index_constituent_snapshots": index_constituents["snapshots"],
        "index_constituent_start": index_constituents["start"],
        "index_constituent_end": index_constituents["end"],
        "index_constituent_weight_unit": "percent",
        "index_constituent_point_in_time": "snapshot_date < session",
        "finance": "finance.sqlite",
        "benchmark_sids": {record.ts_code: record.sid for record in benchmark_records},
    }
    (output_dir / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )


REQUIRED_BUNDLE_ENTRIES = (
    f"assets-{ASSET_DB_VERSION}.sqlite",
    "daily_equities.bcolz",
    "index_daily.bcolz",
    "minute_equities.bcolz",
    "adjustments.sqlite",
    "finance.sqlite",
    "index_constituents.sqlite",
    "manifest.json",
)


def _validate_tualpha_files(path: Path) -> None:
    manifest_path = path / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if int(manifest.get("schema_version", -1)) != BUNDLE_SCHEMA_VERSION:
        raise DataError("Bundle manifest schema version is invalid")
    legacy = path / "tualpha.duckdb"
    if legacy.exists():
        raise DataError(f"legacy Bundle sidecar must not exist: {legacy}")
    daily = bcolz.open(rootdir=str(path / "daily_equities.bcolz"), mode="r")
    if int(daily.attrs["tualpha_schema_version"]) != BUNDLE_SCHEMA_VERSION:
        raise DataError("daily Bcolz extension schema version is invalid")
    fields = dict(daily.attrs["tualpha_fields"])
    required_fields = {
        "pre_close",
        "volume",
        "adj_factor",
        "daily_basic.pe_ttm",
        "moneyflow.net_mf_amount",
        "stock_st.is_st",
    }
    if not required_fields.issubset(fields):
        raise DataError("daily Bcolz extension registry is incomplete")
    for spec in fields.values():
        column = str(spec["column"])
        if column not in daily.names or len(daily[column]) != len(daily):
            raise DataError(f"daily Bcolz extension column is invalid: {column}")
    index_daily = bcolz.open(rootdir=str(path / "index_daily.bcolz"), mode="r")
    if int(index_daily.attrs["tualpha_schema_version"]) != BUNDLE_SCHEMA_VERSION:
        raise DataError("index Bcolz schema version is invalid")
    if any(
        len(index_daily[column]) != len(index_daily) for column in index_daily.names
    ):
        raise DataError("index Bcolz columns have inconsistent lengths")

    finance = sqlite3.connect(path / "finance.sqlite")
    try:
        integrity = finance.execute("PRAGMA integrity_check").fetchone()
        metadata = dict(finance.execute("SELECT key, value FROM finance_metadata"))
    finally:
        finance.close()
    if integrity != ("ok",):
        raise DataError(f"finance.sqlite integrity check failed: {integrity}")
    if metadata.get("schema_version") != str(BUNDLE_SCHEMA_VERSION):
        raise DataError("finance.sqlite schema version is invalid")

    constituents_path = path / "index_constituents.sqlite"
    if not constituents_path.is_file():
        raise DataError("index constituent SQLite is missing")
    constituents = sqlite3.connect(constituents_path)
    try:
        constituent_integrity = constituents.execute(
            "PRAGMA integrity_check"
        ).fetchone()
        constituent_metadata = dict(
            constituents.execute("SELECT key, value FROM index_constituent_metadata")
        )
        invalid_rows = constituents.execute(
            "SELECT count(*) FROM index_constituents "
            "WHERE weight < 0 OR weight > 100 OR snapshot_date > ?",
            [str(manifest["end_session"])],
        ).fetchone()[0]
        codes = [
            str(row[0])
            for row in constituents.execute(
                "SELECT DISTINCT index_code FROM index_constituents ORDER BY index_code"
            )
        ]
        row = constituents.execute(
            "SELECT count(*), "
            "count(DISTINCT index_code || ':' || snapshot_date) "
            "FROM index_constituents"
        ).fetchone()
    finally:
        constituents.close()
    if constituent_integrity != ("ok",):
        raise DataError(
            f"index_constituents.sqlite integrity check failed: {constituent_integrity}"
        )
    if constituent_metadata.get("schema_version") != str(BUNDLE_SCHEMA_VERSION):
        raise DataError("index constituent SQLite schema version is invalid")
    if constituent_metadata.get("generated_at") != str(manifest["generated_at"]):
        raise DataError("index constituent SQLite generation is invalid")
    if constituent_metadata.get("point_in_time_rule") != "snapshot_date < session":
        raise DataError("index constituent point-in-time rule is invalid")
    if invalid_rows:
        raise DataError("index constituent rows are invalid")
    if codes != list(manifest.get("index_constituent_codes", [])):
        raise DataError("index constituent code manifest is inconsistent")
    if int(row[0]) != int(manifest.get("index_constituent_rows", -1)) or int(
        row[1]
    ) != int(manifest.get("index_constituent_snapshots", -1)):
        raise DataError("index constituent counts are inconsistent")


def _write_bundle_files(
    output_dir: Path,
    normalized_path: Path,
    csv_dir: Path,
    records: Sequence[BundleAssetRecord],
    benchmark_records: Sequence[BundleAssetRecord],
    start_session: pd.Timestamp,
    end_session: pd.Timestamp,
    *,
    show_progress: bool,
    bundle_name: str,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=False)
    calendar = get_calendar("XSHG")
    assets_path = output_dir / f"assets-{ASSET_DB_VERSION}.sqlite"
    daily_path = output_dir / "daily_equities.bcolz"
    index_path = output_dir / "index_daily.bcolz"
    minute_path = output_dir / "minute_equities.bcolz"
    adjustments_path = output_dir / "adjustments.sqlite"

    connection = duckdb.connect(str(normalized_path), read_only=True)
    try:
        AssetDBWriter(str(assets_path)).write(
            equities=_asset_frame(records, calendar),
            equity_supplementary_mappings=_asset_supplementary_frame(records),
        )
        daily_writer = BcolzDailyBarWriter(
            str(daily_path), calendar, start_session, end_session
        )
        daily_writer.write(
            _daily_data(connection, records, calendar),
            assets={record.sid for record in records},
            show_progress=show_progress,
            invalid_data_behavior="raise",
        )
        minute_path.mkdir()
        BcolzMinuteBarWriter(
            str(minute_path),
            calendar,
            start_session,
            end_session,
            minutes_per_day=240,
        )
        daily_reader = BcolzDailyBarReader(str(daily_path))
        with SQLiteAdjustmentWriter(
            str(adjustments_path), daily_reader, overwrite=True
        ) as adjustment_writer:
            adjustment_writer.write(mergers=_adjustment_events(connection, records))
    finally:
        connection.close()

    _write_index_bcolz(normalized_path, index_path, benchmark_records)
    _append_daily_extensions(
        normalized_path,
        csv_dir,
        daily_path,
        output_dir,
        records,
    )
    _write_finance(normalized_path, csv_dir, output_dir, records)
    generated_at = datetime.now(ZoneInfo("UTC")).isoformat()
    index_constituents = _write_index_constituents(
        csv_dir,
        output_dir,
        records,
        end_session,
        generated_at,
    )
    _write_manifest(
        output_dir,
        records,
        benchmark_records,
        start_session,
        end_session,
        bundle_name,
        generated_at,
        index_constituents,
    )


class LockedBundleData:
    """BundleData proxy that holds the fixed-layout reader lock."""

    def __init__(self, data: BundleData, lock_key: str) -> None:
        self._data = data
        self._lock_key: str | None = lock_key

    def __getattr__(self, name: str) -> Any:
        return getattr(self._data, name)

    def close(self) -> None:
        if self._lock_key is None:
            return
        try:
            self._data.close()
        finally:
            release_bundle_read_lock(self._lock_key)
            self._lock_key = None

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()


def load_bundle_data(
    bundle_root: str | Path = DEFAULT_BUNDLE_ROOT,
    bundle_name: str = BUNDLE_NAME,
) -> LockedBundleData:
    """Open the fixed-layout bundle with locked Zipline official readers."""

    lock_key, _ = acquire_bundle_read_lock(bundle_root, bundle_name)
    try:
        path = latest_bundle_path(bundle_root, bundle_name)
        data = BundleData(
            asset_finder=ZiplineAssetFinder(
                str(path / f"assets-{ASSET_DB_VERSION}.sqlite")
            ),
            equity_minute_bar_reader=BcolzMinuteBarReader(
                str(path / "minute_equities.bcolz")
            ),
            equity_daily_bar_reader=BcolzDailyBarReader(
                str(path / "daily_equities.bcolz")
            ),
            adjustment_reader=SQLiteAdjustmentReader(str(path / "adjustments.sqlite")),
        )
    except Exception:
        release_bundle_read_lock(lock_key)
        raise
    return LockedBundleData(data, lock_key)


def _reject_interrupted_data_update(root: Path) -> None:
    status_path = update_status_path(root)
    if not status_path.is_file():
        return
    try:
        status = json.loads(status_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return
    if status.get("operation") == "data_update" and status.get("status") == "running":
        raise DataError(
            "a previous data update was interrupted; run `tualpha update` "
            "to recover CSV files before building a Bundle"
        )


def _recover_interrupted_bundle(destination: Path) -> None:
    candidates = sorted(
        destination.parent.glob(f".previous-{destination.name}-*"),
        key=lambda candidate: candidate.stat().st_mtime_ns,
        reverse=True,
    )
    if not destination.exists():
        for candidate in candidates:
            if (candidate / "READY").is_file():
                os.replace(candidate, destination)
                break
    if destination.is_dir() and (destination / "READY").is_file():
        for candidate in candidates:
            if candidate.exists():
                shutil.rmtree(candidate, ignore_errors=True)


def _publish_fixed_bundle(staged: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    previous = destination.parent / f".previous-{destination.name}-{uuid4().hex}"
    moved_previous = False
    try:
        if destination.exists():
            os.replace(destination, previous)
            moved_previous = True
        os.replace(staged, destination)
    except Exception:
        if moved_previous and previous.exists() and not destination.exists():
            os.replace(previous, destination)
        raise
    if moved_previous:
        shutil.rmtree(previous, ignore_errors=True)
    for obsolete in destination.parent.glob(f".previous-{destination.name}-*"):
        shutil.rmtree(obsolete, ignore_errors=True)


def build_bundle(
    csv_dir: str | Path,
    *,
    bundle_root: str | Path = DEFAULT_BUNDLE_ROOT,
    bundle_name: str = BUNDLE_NAME,
    rebuild_normalized: bool = False,
    show_progress: bool = False,
    record_status: bool = True,
    _maintenance_locked: bool = False,
) -> BundleBuildResult:
    """Build from an explicit CSV cache and publish to a fixed directory."""

    source = Path(csv_dir).expanduser()
    root = Path(bundle_root).expanduser()
    validate_bundle_name(bundle_name)
    if paths_overlap(source, root):
        raise DataError(f"csv_dir and bundle_root must not overlap: {source} / {root}")
    started_at = datetime.now(ZoneInfo("UTC")).isoformat()
    destination = bundle_path(root, bundle_name)
    lock_path = bundle_lock_path(root, bundle_name)
    maintenance_path = root / ".locks" / "update.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with ExitStack() as locks:
        if not _maintenance_locked:
            locks.enter_context(FileLock(str(maintenance_path)))
            _reject_interrupted_data_update(root)
        locks.enter_context(FileLock(str(lock_path)))
        _recover_interrupted_bundle(destination)
        store = NormalizedStore(source, root, bundle_name)
        if rebuild_normalized or not store.is_compatible():
            store.rebuild()
        connection = store.connect()
        try:
            min_date, max_date = connection.execute(
                "SELECT min(trade_date), max(trade_date) FROM prices"
            ).fetchone()
            if min_date is None or max_date is None:
                raise DataError("normalized price store is empty")
            start_session = pd.Timestamp(min_date)
            end_session = pd.Timestamp(max_date)
            calendar = get_calendar("XSHG")
            expected = calendar.sessions_in_range(start_session, end_session)
            trade_cal = pd.read_csv(source / "trade_cal.csv", dtype=str)
            local_dates = trade_cal.loc[
                (trade_cal["exchange"] == "SSE")
                & (trade_cal["is_open"] == "1")
                & (trade_cal["cal_date"] >= start_session.strftime("%Y%m%d"))
                & (trade_cal["cal_date"] <= end_session.strftime("%Y%m%d")),
                "cal_date",
            ].drop_duplicates()
            local_sessions = pd.DatetimeIndex(
                pd.to_datetime(local_dates, format="%Y%m%d")
            )
            if not expected.equals(local_sessions):
                raise DataError("local trade_cal does not match Zipline XSHG sessions")
            sid_path = cache_dir(root, bundle_name) / "sid-map.json"
            legacy_sid_paths = (
                source / ".tualpha" / "cache" / bundle_name / "sid-map.json",
                source / "data" / bundle_name / ".cache" / "sid-map.json",
            )
            if not sid_path.exists():
                for legacy_sid_path in legacy_sid_paths:
                    if legacy_sid_path.is_file():
                        sid_path.parent.mkdir(parents=True, exist_ok=True)
                        shutil.copy2(legacy_sid_path, sid_path)
                        break
            sid_registry = SidRegistry(sid_path)
            records = _load_bundle_assets(source, connection, sid_registry, end_session)
            if not records:
                raise DataError("no stock or ETF assets have valid daily bars")
            benchmark_records = _load_benchmark_assets(
                connection,
                sid_registry,
                start_session,
                end_session,
            )
        finally:
            connection.close()

        staging_root = root / ".staging" / f"b-{uuid4().hex[:8]}"
        staged_bundle = staging_root / "bundles" / "b"
        try:
            staging_root.mkdir(parents=True, exist_ok=False)
            _write_bundle_files(
                staged_bundle,
                store.path,
                source,
                records,
                benchmark_records,
                start_session,
                end_session,
                show_progress=show_progress,
                bundle_name=bundle_name,
            )
            missing = [
                filename
                for filename in REQUIRED_BUNDLE_ENTRIES
                if not (staged_bundle / filename).exists()
            ]
            if missing:
                raise DataError(f"staged bundle is missing files: {missing}")
            _validate_tualpha_files(staged_bundle)
            (staged_bundle / "READY").write_text("ok\n", encoding="ascii")
            loaded = load_bundle_data(staging_root, "b")
            loaded.close()
            sid_registry.save()
            _publish_fixed_bundle(staged_bundle, destination)
        finally:
            shutil.rmtree(staging_root, ignore_errors=True)

        sessions = calendar.sessions_in_range(start_session, end_session)
        result = BundleBuildResult(
            path=destination,
            start_session=start_session,
            end_session=end_session,
            asset_count=len(records),
            session_count=len(sessions),
        )
        if record_status:
            _record_bundle_build(
                root,
                bundle_name=bundle_name,
                csv_dir=source,
                started_at=started_at,
                result=result,
            )
        return result
