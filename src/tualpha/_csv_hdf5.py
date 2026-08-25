"""Direct CSV -> staged hash buckets -> sorted HDF5 Bundle writers."""

from __future__ import annotations

import csv
import json
import shutil
import subprocess
import sys
import zlib
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import h5py
import numpy as np
import pandas as pd
import polars as pl

from ._calendar_store import SessionCalendar
from ._hdf5_store import create_compound_dataset, date_to_int, dates_to_int
from .exceptions import DataError
from .tushare_fields import FINANCIAL_FIELDS

_CODE_BYTES = 16
_CATEGORY_BYTES = 128


@dataclass(frozen=True, slots=True)
class BucketSet:
    root: Path
    dtype: np.dtype
    bucket_count: int


def _csv_files(directory: Path) -> list[Path]:
    if not directory.is_dir():
        raise DataError(f"required raw data directory does not exist: {directory}")
    files = sorted(directory.glob("*.csv"))
    if not files:
        raise DataError(f"CSV source directory is empty: {directory}")
    return files


def _read_csv(path: Path, usecols: Sequence[str] | None = None) -> pd.DataFrame:
    try:
        frame = pl.read_csv(
            path,
            columns=usecols,
            infer_schema=False,
            empty_string_is_null=False,
            raise_if_empty=False,
        )
    except (pl.exceptions.NoDataError, pl.exceptions.ComputeError) as exc:
        raise DataError(f"CSV schema is invalid: {path}") from exc
    if not frame.width:
        return pd.DataFrame(columns=usecols)
    return pd.DataFrame(frame.to_dict(as_series=False))


def _file_columns(path: Path) -> list[str]:
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        try:
            return [str(column) for column in next(csv.reader(stream))]
        except StopIteration:
            return []


def _source_columns(directory: Path) -> list[str]:
    for path in _csv_files(directory):
        columns = _file_columns(path)
        if columns:
            return columns
    raise DataError(f"CSV source contains no columns: {directory}")


def _deduplicate_dates(rows: np.ndarray) -> np.ndarray:
    if len(rows) < 2:
        return rows
    keep = np.ones(len(rows), dtype=bool)
    keep[:-1] = rows["trade_date"][:-1] != rows["trade_date"][1:]
    return rows[keep]


def _group_codes(rows: np.ndarray) -> Iterable[tuple[str, np.ndarray]]:
    if not len(rows):
        return
    codes = rows["ts_code"]
    starts = np.r_[0, np.flatnonzero(codes[1:] != codes[:-1]) + 1]
    stops = np.r_[starts[1:], len(rows)]
    for start, stop in zip(starts, stops, strict=True):
        code = bytes(codes[start]).rstrip(b"\0").decode("ascii")
        yield code, rows[start:stop]


def _sink_buckets(
    frame: pl.LazyFrame,
    root: Path,
    dtype: np.dtype,
    bucket_count: int,
) -> BucketSet:
    shutil.rmtree(root, ignore_errors=True)
    expression = pl.col("ts_code").hash(seed=0).mod(bucket_count).cast(pl.UInt16)
    frame.with_columns(expression.alias("_bucket")).sink_parquet(
        pl.PartitionBy(root, key="_bucket", include_key=False),
        compression="zstd",
        compression_level=1,
        maintain_order=True,
        mkdir=True,
        engine="streaming",
    )
    return BucketSet(root, dtype, bucket_count)


def _structured_from_polars(frame: pl.DataFrame, dtype: np.dtype) -> np.ndarray:
    rows = np.empty(frame.height, dtype=dtype)
    for field in dtype.names or ():
        if field not in frame.columns:
            raise DataError(f"staging bucket is missing field: {field}")
        target = dtype[field]
        series = frame[field]
        if target.kind == "S":
            size = target.itemsize
            encoded = [str(value or "").encode("utf-8") for value in series.to_list()]
            if any(len(value) > size for value in encoded):
                raise DataError(f"staging value exceeds {size} bytes: {field}")
            rows[field] = np.asarray(encoded, dtype=f"S{size}")
        else:
            values = series.to_numpy()
            if series.null_count():
                if target.kind == "f":
                    values = series.fill_null(float("nan")).to_numpy()
                else:
                    raise DataError(f"staging bucket has null key/flag: {field}")
            rows[field] = np.asarray(values, dtype=target)
    return rows


def _materialize_sorted_buckets(
    buckets: BucketSet,
    sort_fields: Sequence[str],
) -> None:
    destination = buckets.root / "sorted"
    shutil.rmtree(destination, ignore_errors=True)
    destination.mkdir()
    for bucket in range(buckets.bucket_count):
        directory = buckets.root / f"_bucket={bucket}"
        files = sorted(directory.glob("*.parquet")) if directory.is_dir() else []
        path = destination / f"{bucket:03d}.bin"
        if not files:
            path.touch()
            continue
        frame = (
            pl.scan_parquet(files).sort(list(sort_fields)).collect(engine="streaming")
        )
        rows = _structured_from_polars(frame, buckets.dtype)
        rows.tofile(path)
        del rows, frame
        shutil.rmtree(directory, ignore_errors=True)


def _sorted_bucket_rows(
    buckets: BucketSet,
    bucket: int,
    sort_fields: Sequence[str],
) -> np.ndarray:
    del sort_fields
    path = buckets.root / "sorted" / f"{bucket:03d}.bin"
    if not path.is_file():
        raise DataError(f"sorted staging bucket is missing: {path}")
    if path.stat().st_size % buckets.dtype.itemsize:
        raise DataError(f"sorted staging bucket is truncated: {path}")
    return np.fromfile(path, dtype=buckets.dtype)


def _sessions_for_record(record: Any, calendar: SessionCalendar) -> pd.DatetimeIndex:
    sessions = calendar.sessions_in_range(record.start_date, record.end_date)
    if not len(sessions):
        raise DataError(f"asset has no bundled sessions: {record.ts_code}")
    return sessions


def _empty_daily_array(sessions: pd.DatetimeIndex, dtype: np.dtype) -> np.ndarray:
    values = np.empty(len(sessions), dtype=dtype)
    values["trade_date"] = dates_to_int(sessions)
    for field in dtype.names or ():
        if field == "trade_date":
            continue
        kind = dtype[field].kind
        if kind == "f":
            values[field] = np.nan
        elif kind == "i":
            values[field] = -1
        else:
            values[field] = 0
    return values


def _place_rows(
    target: np.ndarray,
    rows: np.ndarray,
    value_fields: Sequence[str],
) -> None:
    dates = target["trade_date"]
    positions = np.searchsorted(dates, rows["trade_date"])
    valid = positions < len(dates)
    valid[valid] &= dates[positions[valid]] == rows["trade_date"][valid]
    if not valid.all():
        bad = rows["trade_date"][~valid][:5].tolist()
        raise DataError(f"daily rows fall outside the asset calendar: {bad}")
    for field in value_fields:
        target[field][positions] = rows[field]


_DAILY_BUCKET_DTYPE = np.dtype(
    [
        ("ts_code", f"S{_CODE_BYTES}"),
        ("asset_type", "u1"),
        ("trade_date", "<i4"),
        ("source_order", "<u8"),
        ("open", "<f8"),
        ("high", "<f8"),
        ("low", "<f8"),
        ("close", "<f8"),
        ("pre_close", "<f8"),
        ("volume", "<f8"),
        ("turnover", "<f8"),
    ]
)
_DAILY_FINAL_DTYPE = np.dtype(
    [
        ("trade_date", "<i4"),
        ("open", "<f8"),
        ("high", "<f8"),
        ("low", "<f8"),
        ("close", "<f8"),
        ("pre_close", "<f8"),
        ("volume", "<f8"),
        ("turnover", "<f8"),
    ]
)
_DAILY_FIELDS = ("open", "high", "low", "close", "pre_close", "volume", "turnover")
_ASSET_TYPE_IDS = {"stock": 0, "etf": 1, "index": 2}
_ASSET_TYPE_NAMES = {value: key for key, value in _ASSET_TYPE_IDS.items()}


def _bucket_daily_inprocess(
    csv_dir: Path,
    staging: Path,
    *,
    show_progress: bool,
    bucket_count: int = 16,
) -> tuple[BucketSet, dict[tuple[str, str], tuple[pd.Timestamp, pd.Timestamp]]]:
    del show_progress
    required = {
        "ts_code",
        "trade_date",
        "open",
        "high",
        "low",
        "close",
        "pre_close",
        "vol",
        "amount",
    }
    sources = (
        ("daily", "stock", 100.0),
        ("fund_daily", "etf", 100.0),
        ("index_daily", "index", 1.0),
    )
    frames: list[pl.LazyFrame] = []
    for dirname, asset_type, volume_multiplier in sources:
        directory = csv_dir / dirname
        missing = required.difference(_source_columns(directory))
        if missing:
            raise DataError(f"daily CSV is missing {sorted(missing)}: {directory}")
        frame = pl.scan_csv(
            str(directory / "*.csv"),
            infer_schema=False,
            empty_string_is_null=False,
            raise_if_empty=True,
        ).select(
            pl.col("ts_code").str.to_uppercase(),
            pl.lit(_ASSET_TYPE_IDS[asset_type], dtype=pl.UInt8).alias("asset_type"),
            pl.col("trade_date").cast(pl.Int32, strict=False),
            *(
                pl.col(field).cast(pl.Float64, strict=False)
                for field in ("open", "high", "low", "close", "pre_close")
            ),
            (pl.col("vol").cast(pl.Float64, strict=False) * volume_multiplier).alias(
                "volume"
            ),
            (pl.col("amount").cast(pl.Float64, strict=False) * 1000.0).alias(
                "turnover"
            ),
        )
        frames.append(frame)
    combined = (
        pl.concat(frames)
        .with_row_index("source_order")
        .with_columns(pl.col("source_order").cast(pl.UInt64))
    )
    buckets = _sink_buckets(
        combined, staging / "daily", _DAILY_BUCKET_DTYPE, bucket_count
    )
    parsed: dict[tuple[str, str], tuple[pd.Timestamp, pd.Timestamp]] = {}
    for bucket in range(bucket_count):
        files = sorted((buckets.root / f"_bucket={bucket}").glob("*.parquet"))
        if not files:
            continue
        observations = (
            pl.scan_parquet(files)
            .group_by("ts_code", "asset_type")
            .agg(
                pl.col("trade_date").min().alias("first_date"),
                pl.col("trade_date").max().alias("last_date"),
            )
            .collect(engine="streaming")
        )
        for row in observations.iter_rows(named=True):
            parsed[
                (
                    str(row["ts_code"]),
                    _ASSET_TYPE_NAMES[int(row["asset_type"])],
                )
            ] = (
                pd.to_datetime(str(row["first_date"]), format="%Y%m%d"),
                pd.to_datetime(str(row["last_date"]), format="%Y%m%d"),
            )
    _materialize_sorted_buckets(buckets, ("ts_code", "trade_date", "source_order"))
    return buckets, parsed


def write_daily(
    buckets: BucketSet,
    handle: h5py.File,
    records: Sequence[Any],
    benchmark_records: Sequence[Any],
    calendar: SessionCalendar,
) -> int:
    by_code = {str(record.ts_code): record for record in [*records, *benchmark_records]}
    tradable = {str(record.ts_code) for record in records}
    rows_written = 0
    written: set[str] = set()
    for bucket in range(buckets.bucket_count):
        rows = _sorted_bucket_rows(
            buckets, bucket, ("ts_code", "trade_date", "source_order")
        )
        for code, code_rows in _group_codes(rows):
            record = by_code.get(code)
            if record is None:
                continue
            code_rows = _deduplicate_dates(code_rows)
            if code in tradable:
                sessions = _sessions_for_record(record, calendar)
                output = _empty_daily_array(sessions, _DAILY_FINAL_DTYPE)
                code_rows = code_rows[
                    (code_rows["trade_date"] >= output["trade_date"][0])
                    & (code_rows["trade_date"] <= output["trade_date"][-1])
                ]
                _place_rows(output, code_rows, _DAILY_FIELDS)
                output["volume"][~np.isfinite(output["volume"])] = 0.0
                output["turnover"][~np.isfinite(output["turnover"])] = 0.0
            else:
                output = np.empty(len(code_rows), dtype=_DAILY_FINAL_DTYPE)
                for field in output.dtype.names or ():
                    output[field] = code_rows[field]
            create_compound_dataset(handle["data"], code, output)
            written.add(code)
            rows_written += len(output)
    missing = set(by_code).difference(written)
    if missing:
        raise DataError(
            f"daily CSV contains no rows for bundled assets: {sorted(missing)[:5]}"
        )
    return rows_written


@dataclass(frozen=True, slots=True)
class DailyRole:
    role: str
    directories: tuple[str, ...]
    numeric_fields: tuple[str, ...] = ()
    categorical_fields: tuple[str, ...] = ()
    flag_fields: tuple[str, ...] = ()


DAILY_ROLES = (
    DailyRole("adj_factor", ("adj_factor", "fund_adj"), ("adj_factor",)),
    DailyRole(
        "daily_basic",
        ("daily_basic",),
        (
            "close",
            "turnover_rate",
            "turnover_rate_f",
            "volume_ratio",
            "pe",
            "pe_ttm",
            "pb",
            "ps",
            "ps_ttm",
            "dv_ratio",
            "dv_ttm",
            "total_share",
            "float_share",
            "free_share",
            "total_mv",
            "circ_mv",
            "limit_status",
        ),
    ),
    DailyRole("stk_limit", ("stk_limit",), ("up_limit", "down_limit")),
    DailyRole(
        "industry",
        ("industry",),
        (),
        ("l1_code", "l1_name", "l2_code", "l2_name", "l3_code", "l3_name"),
    ),
    DailyRole(
        "stock_st",
        ("stock_st",),
        (),
        ("name", "type", "type_name"),
        ("is_st",),
    ),
    DailyRole(
        "moneyflow",
        ("moneyflow",),
        (
            "buy_sm_vol",
            "buy_sm_amount",
            "sell_sm_vol",
            "sell_sm_amount",
            "buy_md_vol",
            "buy_md_amount",
            "sell_md_vol",
            "sell_md_amount",
            "buy_lg_vol",
            "buy_lg_amount",
            "sell_lg_vol",
            "sell_lg_amount",
            "buy_elg_vol",
            "buy_elg_amount",
            "sell_elg_vol",
            "sell_elg_amount",
            "net_mf_vol",
            "net_mf_amount",
        ),
    ),
    DailyRole("suspend_d", ("suspend_d",), (), (), ("suspended",)),
)


def _role_bucket_dtype(role: DailyRole) -> np.dtype:
    return np.dtype(
        [
            ("ts_code", f"S{_CODE_BYTES}"),
            ("trade_date", "<i4"),
            ("source_order", "<u8"),
            *[(field, "<f8") for field in role.numeric_fields],
            *[(field, f"S{_CATEGORY_BYTES}") for field in role.categorical_fields],
            *[(field, "u1") for field in role.flag_fields],
        ]
    )


def _bucket_daily_role_inprocess(
    csv_dir: Path,
    staging: Path,
    role: DailyRole,
    *,
    show_progress: bool,
    bucket_count: int = 16,
) -> tuple[BucketSet, dict[str, list[str]]]:
    del show_progress
    dtype = _role_bucket_dtype(role)
    frames: list[pl.LazyFrame] = []
    for dirname in role.directories:
        directory = csv_dir / dirname
        columns = set(_source_columns(directory))
        if not {"ts_code", "trade_date"}.issubset(columns):
            raise DataError(f"{role.role} CSV is missing keys: {directory}")
        frame = pl.scan_csv(
            str(directory / "*.csv"),
            infer_schema=False,
            empty_string_is_null=False,
            raise_if_empty=True,
        )
        if role.role == "suspend_d" and "suspend_type" in columns:
            frame = frame.filter(pl.col("suspend_type").str.to_uppercase() == "S")
        expressions: list[pl.Expr] = [
            pl.col("ts_code").str.to_uppercase(),
            pl.col("trade_date").cast(pl.Int32, strict=False),
        ]
        expressions.extend(
            (
                pl.col(field).cast(pl.Float64, strict=False)
                if field in columns
                else pl.lit(None, dtype=pl.Float64).alias(field)
            )
            for field in role.numeric_fields
        )
        expressions.extend(
            (
                pl.col(field).fill_null("").cast(pl.String)
                if field in columns
                else pl.lit("", dtype=pl.String).alias(field)
            )
            for field in role.categorical_fields
        )
        for field in role.flag_fields:
            if (role.role, field) in {
                ("stock_st", "is_st"),
                ("suspend_d", "suspended"),
            }:
                expressions.append(pl.lit(1, dtype=pl.UInt8).alias(field))
            elif field in columns:
                expressions.append(
                    pl.col(field).cast(pl.UInt8, strict=False).fill_null(0)
                )
            else:
                expressions.append(pl.lit(0, dtype=pl.UInt8).alias(field))
        frames.append(frame.select(expressions))
    combined = (
        pl.concat(frames)
        .with_row_index("source_order")
        .with_columns(pl.col("source_order").cast(pl.UInt64))
    )
    buckets = _sink_buckets(combined, staging / role.role, dtype, bucket_count)
    parquet_glob = str(buckets.root / "_bucket=*" / "*.parquet")
    categories: dict[str, list[str]] = {}
    if list(buckets.root.rglob("*.parquet")):
        source = pl.scan_parquet(parquet_glob)
        for field in role.categorical_fields:
            categories[field] = (
                source.select(field)
                .filter(pl.col(field) != "")
                .unique()
                .sort(field)
                .collect(engine="streaming")[field]
                .to_list()
            )
    else:
        categories = {field: [] for field in role.categorical_fields}
    _materialize_sorted_buckets(buckets, ("ts_code", "trade_date", "source_order"))
    return buckets, categories


def _role_final_dtype(role: DailyRole) -> np.dtype:
    return np.dtype(
        [
            ("trade_date", "<i4"),
            *[(field, "<f8") for field in role.numeric_fields],
            *[(field, "<i4") for field in role.categorical_fields],
            *[(field, "u1") for field in role.flag_fields],
        ]
    )


def write_daily_role(
    buckets: BucketSet,
    handle: h5py.File,
    role: DailyRole,
    categories: Mapping[str, Sequence[str]],
    records: Sequence[Any],
    calendar: SessionCalendar,
) -> int:
    by_code = {str(record.ts_code): record for record in records}
    final_dtype = _role_final_dtype(role)
    value_fields = (
        *role.numeric_fields,
        *role.categorical_fields,
        *role.flag_fields,
    )
    category_maps = {
        field: {value: index for index, value in enumerate(values)}
        for field, values in categories.items()
    }
    written: set[str] = set()
    rows_written = 0
    specs: dict[str, dict[str, str]] = {}
    for field in role.numeric_fields:
        specs[field] = {"kind": "numeric"}
    for field in role.categorical_fields:
        specs[field] = {"kind": "categorical"}
    for field in role.flag_fields:
        specs[field] = {"kind": "flag"}
    handle.attrs["fields"] = json.dumps(specs, ensure_ascii=False)
    dictionary = handle.create_group("dictionary")
    string_type = h5py.string_dtype(encoding="utf-8")
    for field, values in categories.items():
        dictionary.create_dataset(
            field, data=np.asarray(values, dtype=object), dtype=string_type
        )

    for bucket in range(buckets.bucket_count):
        rows = _sorted_bucket_rows(
            buckets, bucket, ("ts_code", "trade_date", "source_order")
        )
        for code, code_rows in _group_codes(rows):
            record = by_code.get(code)
            if record is None:
                continue
            code_rows = _deduplicate_dates(code_rows)
            sessions = _sessions_for_record(record, calendar)
            output = _empty_daily_array(sessions, final_dtype)
            code_rows = code_rows[
                (code_rows["trade_date"] >= output["trade_date"][0])
                & (code_rows["trade_date"] <= output["trade_date"][-1])
            ]
            converted = np.empty(len(code_rows), dtype=final_dtype)
            converted["trade_date"] = code_rows["trade_date"]
            for field in role.numeric_fields:
                converted[field] = code_rows[field]
            for field in role.categorical_fields:
                mapping = category_maps[field]
                converted[field] = np.fromiter(
                    (
                        mapping.get(bytes(value).rstrip(b"\0").decode("utf-8"), -1)
                        for value in code_rows[field]
                    ),
                    dtype=np.int32,
                    count=len(code_rows),
                )
            for field in role.flag_fields:
                converted[field] = code_rows[field]
            _place_rows(output, converted, value_fields)
            create_compound_dataset(handle["data"], code, output)
            written.add(code)
            rows_written += len(output)

    for code in sorted(set(by_code).difference(written)):
        record = by_code[code]
        sessions = _sessions_for_record(record, calendar)
        output = _empty_daily_array(sessions, final_dtype)
        create_compound_dataset(handle["data"], code, output)
        rows_written += len(output)
    return rows_written


_FINANCIAL_KEYS = {
    "ts_code",
    "ann_date",
    "f_ann_date",
    "end_date",
    "report_type",
    "comp_type",
    "end_type",
    "update_flag",
}


def _finance_fields(table: str) -> list[str]:
    return [field for field in FINANCIAL_FIELDS[table] if field not in _FINANCIAL_KEYS]


def _finance_bucket_dtype(table: str) -> np.dtype:
    return np.dtype(
        [
            ("ts_code", f"S{_CODE_BYTES}"),
            ("ann_date", "<i4"),
            ("f_ann_date", "<i4"),
            ("effective_ann_date", "<i4"),
            ("end_date", "<i4"),
            ("report_type", "S16"),
            ("comp_type", "S16"),
            ("end_type", "S16"),
            ("update_flag", "S16"),
            ("source_order", "<u8"),
            *[(field, "<f8") for field in _finance_fields(table)],
        ]
    )


def _bucket_finance_inprocess(
    csv_dir: Path,
    staging: Path,
    table: str,
    *,
    show_progress: bool,
    bucket_count: int = 64,
) -> BucketSet:
    del show_progress
    directory = csv_dir / table
    dtype = _finance_bucket_dtype(table)
    files = sorted(directory.glob("*.csv")) if directory.is_dir() else []
    destination = staging / f"finance-{table}"
    if not files:
        destination.mkdir(parents=True, exist_ok=True)
        buckets = BucketSet(destination, dtype, bucket_count)
        _materialize_sorted_buckets(buckets, ())
        return buckets
    frames: list[pl.LazyFrame] = []
    for path in files:
        columns = set(_file_columns(path))
        required = {"ts_code", "ann_date", "end_date"}
        missing = required.difference(columns)
        if missing:
            raise DataError(f"{table} CSV is missing {sorted(missing)}: {path}")
        source = pl.scan_csv(
            path,
            infer_schema=False,
            empty_string_is_null=False,
            raise_if_empty=False,
        )
        expressions: list[pl.Expr] = [
            pl.col("ts_code").str.to_uppercase(),
            pl.col("ann_date").cast(pl.Int32, strict=False),
            (
                pl.col("f_ann_date").cast(pl.Int32, strict=False).fill_null(0)
                if "f_ann_date" in columns
                else pl.lit(0, dtype=pl.Int32).alias("f_ann_date")
            ),
            pl.col("end_date").cast(pl.Int32, strict=False),
        ]
        expressions.extend(
            (
                pl.col(field).fill_null("").cast(pl.String)
                if field in columns
                else pl.lit("", dtype=pl.String).alias(field)
            )
            for field in ("report_type", "comp_type", "end_type", "update_flag")
        )
        expressions.extend(
            (
                pl.col(field).cast(pl.Float64, strict=False)
                if field in columns
                else pl.lit(None, dtype=pl.Float64).alias(field)
            )
            for field in _finance_fields(table)
        )
        frames.append(source.select(expressions))
    combined = (
        pl.concat(frames)
        .with_columns(
            pl.when(pl.col("f_ann_date") > 0)
            .then(pl.col("f_ann_date"))
            .otherwise(pl.col("ann_date"))
            .alias("effective_ann_date")
        )
        .with_row_index("source_order")
        .with_columns(pl.col("source_order").cast(pl.UInt64))
    )
    buckets = _sink_buckets(combined, destination, dtype, bucket_count)
    _materialize_sorted_buckets(
        buckets,
        ("ts_code", "end_date", "effective_ann_date", "source_order"),
    )
    return buckets


def write_finance_table(
    buckets: BucketSet,
    group: h5py.Group,
    table: str,
    allowed_codes: set[str],
) -> int:
    dtype = _finance_bucket_dtype(table)
    final_dtype = np.dtype(
        [(name, dtype[name]) for name in dtype.names or () if name != "ts_code"]
    )
    total = 0
    for bucket in range(buckets.bucket_count):
        rows = _sorted_bucket_rows(
            buckets,
            bucket,
            (
                "ts_code",
                "end_date",
                "effective_ann_date",
                "source_order",
            ),
        )
        for code, code_rows in _group_codes(rows):
            if code not in allowed_codes:
                continue
            output = np.empty(len(code_rows), dtype=final_dtype)
            for field in final_dtype.names or ():
                output[field] = code_rows[field]
            create_compound_dataset(group, code, output, chunk_rows=256)
            total += len(output)
    return total


def write_index_weights(
    csv_dir: Path,
    handle: h5py.File,
    sid_by_code: Mapping[str, int],
    end_session: pd.Timestamp,
) -> tuple[int, list[str]]:
    source = csv_dir / "index_weight"
    frames: list[pd.DataFrame] = []
    for path in sorted(source.glob("*.csv")) if source.is_dir() else ():
        frame = _read_csv(path)
        if frame.empty:
            continue
        required = {"index_code", "con_code", "trade_date", "weight"}
        missing = required.difference(frame.columns)
        if missing:
            raise DataError(f"index weight CSV is missing {sorted(missing)}: {path}")
        frames.append(frame[list(required)])
    if not frames:
        return 0, []
    frame = pd.concat(frames, ignore_index=True)
    frame["index_code"] = frame["index_code"].str.upper()
    frame["con_code"] = frame["con_code"].str.upper()
    frame["snapshot_date"] = pd.to_numeric(frame["trade_date"], errors="coerce")
    frame["weight"] = pd.to_numeric(frame["weight"], errors="coerce")
    frame = frame[
        frame["snapshot_date"].notna()
        & frame["snapshot_date"].le(date_to_int(end_session))
    ].copy()
    invalid = (
        frame["index_code"].eq("")
        | frame["con_code"].eq("")
        | ~frame["weight"].between(0.0, 100.0)
    )
    if invalid.any():
        raise DataError("index weight CSV contains invalid rows")
    frame = frame.drop_duplicates(
        ["index_code", "snapshot_date", "con_code"], keep="last"
    ).sort_values(["index_code", "snapshot_date", "con_code"])
    dtype = np.dtype(
        [
            ("snapshot_date", "<i4"),
            ("con_code", "S16"),
            ("sid", "<i8"),
            ("weight", "<f8"),
        ]
    )
    total = 0
    codes: list[str] = []
    for index_code, rows in frame.groupby("index_code", sort=True):
        values = np.empty(len(rows), dtype=dtype)
        values["snapshot_date"] = rows["snapshot_date"].to_numpy(dtype=np.int32)
        values["con_code"] = rows["con_code"].str.encode("ascii").to_numpy()
        values["sid"] = np.fromiter(
            (sid_by_code.get(code, -1) for code in rows["con_code"]),
            dtype=np.int64,
            count=len(rows),
        )
        values["weight"] = rows["weight"].to_numpy(dtype=float)
        create_compound_dataset(handle["data"], str(index_code), values)
        total += len(values)
        codes.append(str(index_code))
    return total, codes


_WORKER_FILE_THRESHOLD = 256


def _run_bucket_worker(staging: Path, payload: dict[str, Any]) -> dict[str, Any]:
    staging.mkdir(parents=True, exist_ok=True)
    run_id = zlib.crc32(json.dumps(payload, sort_keys=True).encode("utf-8"))
    config_path = staging / f".worker-{run_id:08x}.json"
    result_path = staging / f".worker-{run_id:08x}.result.json"
    payload = {**payload, "result_path": str(result_path)}
    config_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    try:
        completed = subprocess.run(
            [sys.executable, "-m", "tualpha._csv_bucket_worker", str(config_path)],
            check=False,
            capture_output=True,
            text=True,
        )
        if completed.returncode != 0:
            detail = completed.stderr.strip() or completed.stdout.strip()
            raise DataError(
                f"CSV bucket worker failed with exit code {completed.returncode}: "
                f"{detail[-2000:]}"
            )
        if not result_path.is_file():
            raise DataError("CSV bucket worker did not write its result")
        return json.loads(result_path.read_text(encoding="utf-8"))
    finally:
        config_path.unlink(missing_ok=True)
        result_path.unlink(missing_ok=True)


def bucket_daily(
    csv_dir: Path,
    staging: Path,
    *,
    show_progress: bool,
    bucket_count: int = 16,
) -> tuple[BucketSet, dict[tuple[str, str], tuple[pd.Timestamp, pd.Timestamp]]]:
    file_count = sum(
        len(_csv_files(csv_dir / directory))
        for directory in ("daily", "fund_daily", "index_daily")
    )
    if file_count <= _WORKER_FILE_THRESHOLD:
        return _bucket_daily_inprocess(
            csv_dir,
            staging,
            show_progress=show_progress,
            bucket_count=bucket_count,
        )
    result = _run_bucket_worker(
        staging,
        {
            "action": "daily",
            "csv_dir": str(csv_dir),
            "staging": str(staging),
            "bucket_count": bucket_count,
        },
    )
    observations = {
        (str(row["ts_code"]), str(row["asset_type"])): (
            pd.to_datetime(str(row["first_date"]), format="%Y%m%d"),
            pd.to_datetime(str(row["last_date"]), format="%Y%m%d"),
        )
        for row in result["observations"]
    }
    return BucketSet(staging / "daily", _DAILY_BUCKET_DTYPE, bucket_count), observations


def bucket_daily_role(
    csv_dir: Path,
    staging: Path,
    role: DailyRole,
    *,
    show_progress: bool,
    bucket_count: int = 16,
) -> tuple[BucketSet, dict[str, list[str]]]:
    file_count = sum(
        len(_csv_files(csv_dir / directory)) for directory in role.directories
    )
    if file_count <= _WORKER_FILE_THRESHOLD:
        return _bucket_daily_role_inprocess(
            csv_dir,
            staging,
            role,
            show_progress=show_progress,
            bucket_count=bucket_count,
        )
    result = _run_bucket_worker(
        staging,
        {
            "action": "daily_role",
            "role": role.role,
            "csv_dir": str(csv_dir),
            "staging": str(staging),
            "bucket_count": bucket_count,
        },
    )
    return (
        BucketSet(staging / role.role, _role_bucket_dtype(role), bucket_count),
        {
            str(field): [str(value) for value in values]
            for field, values in result["categories"].items()
        },
    )


def bucket_finance(
    csv_dir: Path,
    staging: Path,
    table: str,
    *,
    show_progress: bool,
    bucket_count: int = 64,
) -> BucketSet:
    directory = csv_dir / table
    file_count = len(list(directory.glob("*.csv"))) if directory.is_dir() else 0
    if file_count <= _WORKER_FILE_THRESHOLD:
        return _bucket_finance_inprocess(
            csv_dir,
            staging,
            table,
            show_progress=show_progress,
            bucket_count=bucket_count,
        )
    _run_bucket_worker(
        staging,
        {
            "action": "finance",
            "table": table,
            "csv_dir": str(csv_dir),
            "staging": str(staging),
            "bucket_count": bucket_count,
        },
    )
    return BucketSet(
        staging / f"finance-{table}",
        _finance_bucket_dtype(table),
        bucket_count,
    )
