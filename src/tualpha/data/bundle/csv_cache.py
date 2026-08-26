"""Per-run partitioned CSV cache for Tushare update downloads."""

from __future__ import annotations

import json
import os
import shutil
import zlib
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any, Self
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from ...exceptions import DataError
from .parquet_store import sha256_file
from .schema import (
    DAILY_ROLES,
    DailyRole,
    finance_fields,
    finance_update_dtype,
    role_update_dtype,
)

CSV_CACHE_PROTOCOL = "tualpha.update-csv/1"
CODE_BYTES = 16
CATEGORY_BYTES = 128
DAILY_BUCKETS = 16
FINANCE_BUCKETS = 64
INDEX_BUCKETS = 16
CSV_CHUNK_ROWS = 2_048
ASSET_TYPE_IDS = {"stock": 0, "etf": 1, "index": 2}
ASSET_TYPE_NAMES = {value: key for key, value in ASSET_TYPE_IDS.items()}
ROLE_BY_NAME = {role.role: role for role in DAILY_ROLES}

DAILY_UPDATE_DTYPE = np.dtype(
    [
        ("ts_code", f"S{CODE_BYTES}"),
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
INDEX_WEIGHT_UPDATE_DTYPE = np.dtype(
    [
        ("index_code", f"S{CODE_BYTES}"),
        ("con_code", f"S{CODE_BYTES}"),
        ("snapshot_date", "<i4"),
        ("weight", "<f8"),
        ("source_order", "<u8"),
    ]
)


def bucket_for(code: str | bytes, count: int) -> int:
    payload = code if isinstance(code, bytes) else code.encode("ascii")
    return zlib.crc32(payload.rstrip(b"\0")) % count


def decode_code(value: object) -> str:
    if isinstance(value, bytes):
        return value.rstrip(b"\0").decode("ascii")
    if isinstance(value, np.bytes_):
        return bytes(value).rstrip(b"\0").decode("ascii")
    return str(value)


def decode_category(value: object) -> str:
    if isinstance(value, (bytes, np.bytes_)):
        return bytes(value).rstrip(b"\0").decode("utf-8")
    return str(value)


def encode_strings(
    values: pd.Series, size: int, field: str, *, uppercase: bool = False
) -> np.ndarray:
    encoded: list[bytes] = []
    for value in values.fillna("").astype(str):
        text = value.upper() if uppercase else value
        item = text.encode("utf-8")
        if len(item) > size:
            raise DataError(f"downloaded {field} exceeds {size} bytes: {text!r}")
        encoded.append(item)
    return np.asarray(encoded, dtype=f"S{size}")


def integer_dates(frame: pd.DataFrame, field: str) -> np.ndarray:
    values = pd.to_numeric(frame[field], errors="coerce")
    if values.isna().any():
        raise DataError(f"downloaded data contains invalid {field}")
    result = values.to_numpy(dtype=np.int64)
    if np.any((result < 19000101) | (result > 29991231)):
        raise DataError(f"downloaded data contains invalid {field}")
    return result.astype("<i4")


def numeric(frame: pd.DataFrame, field: str) -> np.ndarray:
    if field not in frame:
        return np.full(len(frame), np.nan, dtype="<f8")
    return pd.to_numeric(frame[field], errors="coerce").to_numpy(dtype="<f8")


def _safe_relative_csv(partition: str | Path) -> Path:
    relative = Path(str(partition).replace("\\", "/"))
    if relative.is_absolute() or ".." in relative.parts or not relative.parts:
        raise DataError(f"invalid CSV cache partition: {partition}")
    if relative.suffix.lower() != ".csv":
        relative = relative.with_name(f"{relative.name}.csv")
    return relative


def _rows_to_frame(rows: np.ndarray) -> pd.DataFrame:
    columns: dict[str, Any] = {}
    for field in rows.dtype.names or ():
        values = rows[field]
        if values.dtype.kind == "S":
            columns[field] = [decode_category(value) for value in values]
        else:
            columns[field] = values
    return pd.DataFrame(columns, columns=list(rows.dtype.names or ()))


def _frame_to_rows(frame: pd.DataFrame, dtype: np.dtype, source: Path) -> np.ndarray:
    expected = list(dtype.names or ())
    missing = set(expected).difference(frame.columns)
    if missing:
        raise DataError(
            f"CSV cache partition is missing columns {sorted(missing)}: {source}"
        )
    rows = np.empty(len(frame), dtype=dtype)
    for field in expected:
        field_dtype = dtype.fields[field][0]
        if field_dtype.kind == "S":
            rows[field] = encode_strings(frame[field], field_dtype.itemsize, field)
            continue
        values = pd.to_numeric(frame[field], errors="coerce")
        if field_dtype.kind == "f":
            rows[field] = values.to_numpy(dtype=field_dtype)
            continue
        if values.isna().any():
            raise DataError(f"CSV cache contains invalid integer {field}: {source}")
        numeric_values = values.to_numpy(dtype=np.float64)
        if np.any(~np.isfinite(numeric_values)) or np.any(
            numeric_values != np.floor(numeric_values)
        ):
            raise DataError(f"CSV cache contains invalid integer {field}: {source}")
        bounds = np.iinfo(field_dtype)
        if np.any((numeric_values < bounds.min) | (numeric_values > bounds.max)):
            raise DataError(f"CSV cache integer {field} is out of range: {source}")
        rows[field] = numeric_values.astype(field_dtype)
    return rows


class CsvUpdateWriter:
    """Write normalized update batches into atomic business-partitioned CSV files."""

    def __init__(
        self,
        root: Path,
        target_dates: Sequence[str] = (),
        *,
        resume: bool = False,
    ) -> None:
        self.root = Path(root)
        self.metadata_path = self.root / "cache-manifest.json"
        self._closed = False
        if resume:
            self._metadata = self._load_metadata()
            requested = [int(value) for value in target_dates]
            existing = [int(value) for value in self._metadata.get("target_dates", [])]
            if requested and existing and requested != existing:
                raise DataError(
                    "CSV cache target dates do not match the update request"
                )
        else:
            shutil.rmtree(self.root, ignore_errors=True)
            self.root.mkdir(parents=True, exist_ok=False)
            self._metadata = {
                "protocol": CSV_CACHE_PROTOCOL,
                "created_at": datetime.now(ZoneInfo("UTC")).isoformat(),
                "complete": False,
                "target_dates": [int(value) for value in target_dates],
                "index_weight_coverage": {},
                "source_orders": {},
                "batch_count": 0,
                "partitions": {},
            }
            self._write_metadata()
        self._orders = {
            str(key): int(value)
            for key, value in self._metadata.get("source_orders", {}).items()
        }
        self.batch_count = int(self._metadata.get("batch_count", 0))

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close(success=exc_type is None)

    def _load_metadata(self) -> dict[str, Any]:
        try:
            metadata = json.loads(self.metadata_path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise DataError(
                f"invalid CSV cache manifest: {self.metadata_path}"
            ) from exc
        if metadata.get("protocol") != CSV_CACHE_PROTOCOL:
            raise DataError(f"unsupported CSV cache protocol: {self.metadata_path}")
        if not isinstance(metadata.get("partitions"), dict):
            raise DataError(f"invalid CSV cache partitions: {self.metadata_path}")
        return metadata

    def _write_metadata(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        temporary = self.metadata_path.with_suffix(".tmp")
        temporary.write_text(
            json.dumps(self._metadata, ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        os.replace(temporary, self.metadata_path)

    def close(self, *, success: bool = True) -> None:
        if self._closed:
            return
        self._metadata["complete"] = bool(success)
        self._metadata["closed_at"] = datetime.now(ZoneInfo("UTC")).isoformat()
        self._write_metadata()
        self._closed = True

    def set_target_dates(self, dates: Sequence[str]) -> None:
        values = [int(value) for value in dates]
        existing = [int(value) for value in self._metadata.get("target_dates", [])]
        if existing and existing != values:
            raise DataError("CSV cache target dates cannot change during an update")
        self._metadata["target_dates"] = values
        self._write_metadata()

    def set_index_coverage(self, coverage: Mapping[str, Any]) -> None:
        self._metadata["index_weight_coverage"] = dict(coverage)
        self._write_metadata()

    def _source_orders(self, key: str, count: int) -> np.ndarray:
        start = self._orders.get(key, 0)
        self._orders[key] = start + count
        return np.arange(start, start + count, dtype="<u8")

    def _write_frame(
        self,
        partition: str | Path,
        frame: pd.DataFrame,
        *,
        key: str | None,
        code_field: str | None,
        bucket_count: int | None,
    ) -> Path:
        relative = _safe_relative_csv(partition)
        destination = self.root / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = destination.with_suffix(".csv.tmp")
        frame.to_csv(
            temporary,
            index=False,
            encoding="utf-8",
            lineterminator="\n",
            float_format="%.17g",
        )
        os.replace(temporary, destination)
        self.batch_count += 1
        self._metadata["batch_count"] = self.batch_count
        self._metadata["source_orders"] = dict(self._orders)
        self._metadata["complete"] = False
        self._metadata["partitions"][relative.as_posix()] = {
            "key": key,
            "code_field": code_field,
            "bucket_count": bucket_count,
            "rows": len(frame),
            "columns": list(frame.columns),
            "size": destination.stat().st_size,
            "sha256": sha256_file(destination),
        }
        self._write_metadata()
        return destination

    def write_raw_partition(self, partition: str | Path, frame: pd.DataFrame) -> Path:
        return self._write_frame(
            partition,
            frame,
            key=None,
            code_field=None,
            bucket_count=None,
        )

    def has_partition(self, partition: str | Path, *, verify_hash: bool = True) -> bool:
        relative = _safe_relative_csv(partition)
        raw = self._metadata["partitions"].get(relative.as_posix())
        if not isinstance(raw, dict):
            return False
        path = self.root / relative
        if not path.is_file() or path.stat().st_size != int(raw.get("size", -1)):
            return False
        return not verify_hash or sha256_file(path) == str(raw.get("sha256", ""))

    def read_partition(self, partition: str | Path) -> pd.DataFrame:
        relative = _safe_relative_csv(partition)
        raw = self._metadata["partitions"].get(relative.as_posix())
        if not isinstance(raw, dict):
            raise DataError(f"CSV cache partition is missing: {relative}")
        path = self.root / relative
        if not self.has_partition(relative):
            raise DataError(f"CSV cache partition is invalid: {relative}")
        try:
            return pd.read_csv(path, dtype=str, keep_default_na=False)
        except pd.errors.EmptyDataError:
            return pd.DataFrame(columns=list(raw.get("columns", [])))

    def read_raw_partition(self, partition: str | Path) -> pd.DataFrame:
        relative = _safe_relative_csv(partition)
        raw = self._metadata["partitions"].get(relative.as_posix())
        if not isinstance(raw, dict) or raw.get("key") is not None:
            raise DataError(f"CSV cache master partition is missing: {relative}")
        return self.read_partition(relative)

    def _write_rows(
        self,
        partition: str | Path,
        key: str,
        rows: np.ndarray,
        *,
        code_field: str,
        bucket_count: int,
    ) -> None:
        self._write_frame(
            partition,
            _rows_to_frame(rows),
            key=key,
            code_field=code_field,
            bucket_count=bucket_count,
        )

    def append_daily(
        self,
        frame: pd.DataFrame,
        asset_type: str,
        *,
        partition: str | Path,
    ) -> None:
        if frame.empty:
            rows = np.empty(0, dtype=DAILY_UPDATE_DTYPE)
        else:
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
            missing = required.difference(frame.columns)
            if missing:
                raise DataError(
                    f"downloaded {asset_type} daily data is missing {sorted(missing)}"
                )
            rows = np.empty(len(frame), dtype=DAILY_UPDATE_DTYPE)
            rows["ts_code"] = encode_strings(
                frame["ts_code"], CODE_BYTES, "ts_code", uppercase=True
            )
            rows["asset_type"] = ASSET_TYPE_IDS[asset_type]
            rows["trade_date"] = integer_dates(frame, "trade_date")
            rows["source_order"] = self._source_orders("daily", len(frame))
            for field in ("open", "high", "low", "close", "pre_close"):
                rows[field] = numeric(frame, field)
            multiplier = 1.0 if asset_type == "index" else 100.0
            rows["volume"] = numeric(frame, "vol") * multiplier
            rows["turnover"] = numeric(frame, "amount") * 1000.0
        self._write_rows(
            partition,
            "daily",
            rows,
            code_field="ts_code",
            bucket_count=DAILY_BUCKETS,
        )

    def append_daily_role(
        self,
        role_name: str,
        frame: pd.DataFrame,
        *,
        partition: str | Path,
    ) -> None:
        role = ROLE_BY_NAME[role_name]
        if role_name == "suspend_d" and "suspend_type" in frame:
            frame = frame[
                frame["suspend_type"].fillna("").astype(str).str.upper().eq("S")
            ].reset_index(drop=True)
        dtype = role_update_dtype(role)
        if frame.empty:
            rows = np.empty(0, dtype=dtype)
        else:
            if not {"ts_code", "trade_date"}.issubset(frame.columns):
                raise DataError(f"downloaded {role_name} data is missing daily keys")
            rows = np.empty(len(frame), dtype=dtype)
            rows["ts_code"] = encode_strings(
                frame["ts_code"], CODE_BYTES, "ts_code", uppercase=True
            )
            rows["trade_date"] = integer_dates(frame, "trade_date")
            rows["source_order"] = self._source_orders(role_name, len(frame))
            for field in role.numeric_fields:
                rows[field] = numeric(frame, field)
            for field in role.categorical_fields:
                source = (
                    frame[field]
                    if field in frame
                    else pd.Series([""] * len(frame), index=frame.index)
                )
                rows[field] = encode_strings(source, CATEGORY_BYTES, field)
            for field in role.flag_fields:
                if (role_name, field) in {
                    ("stock_st", "is_st"),
                    ("suspend_d", "suspended"),
                }:
                    rows[field] = 1
                elif field in frame:
                    values = pd.to_numeric(frame[field], errors="coerce").fillna(0)
                    rows[field] = values.to_numpy(dtype="u1")
                else:
                    rows[field] = 0
        self._write_rows(
            partition,
            role_name,
            rows,
            code_field="ts_code",
            bucket_count=DAILY_BUCKETS,
        )

    def append_finance(
        self,
        table: str,
        frame: pd.DataFrame,
        *,
        partition: str | Path,
    ) -> None:
        dtype = finance_update_dtype(table)
        if frame.empty:
            rows = np.empty(0, dtype=dtype)
        else:
            required = {"ts_code", "ann_date", "end_date"}
            missing = required.difference(frame.columns)
            if missing:
                raise DataError(f"downloaded {table} data is missing {sorted(missing)}")
            rows = np.empty(len(frame), dtype=dtype)
            rows["ts_code"] = encode_strings(
                frame["ts_code"], CODE_BYTES, "ts_code", uppercase=True
            )
            rows["ann_date"] = integer_dates(frame, "ann_date")
            if "f_ann_date" in frame:
                f_ann = pd.to_numeric(frame["f_ann_date"], errors="coerce").fillna(0)
                rows["f_ann_date"] = f_ann.to_numpy(dtype="<i4")
            else:
                rows["f_ann_date"] = 0
            rows["effective_ann_date"] = np.where(
                rows["f_ann_date"] > 0, rows["f_ann_date"], rows["ann_date"]
            )
            rows["end_date"] = integer_dates(frame, "end_date")
            for field in ("report_type", "comp_type", "end_type", "update_flag"):
                source = (
                    frame[field]
                    if field in frame
                    else pd.Series([""] * len(frame), index=frame.index)
                )
                rows[field] = encode_strings(source, 16, field)
            rows["source_order"] = self._source_orders(f"finance/{table}", len(frame))
            for field in finance_fields(table):
                rows[field] = numeric(frame, field)
        self._write_rows(
            partition,
            f"finance/{table}",
            rows,
            code_field="ts_code",
            bucket_count=FINANCE_BUCKETS,
        )

    def append_index_weights(
        self, frame: pd.DataFrame, *, partition: str | Path
    ) -> None:
        if frame.empty:
            rows = np.empty(0, dtype=INDEX_WEIGHT_UPDATE_DTYPE)
        else:
            required = {"index_code", "con_code", "trade_date", "weight"}
            missing = required.difference(frame.columns)
            if missing:
                raise DataError(f"downloaded index_weight is missing {sorted(missing)}")
            rows = np.empty(len(frame), dtype=INDEX_WEIGHT_UPDATE_DTYPE)
            rows["index_code"] = encode_strings(
                frame["index_code"], CODE_BYTES, "index_code", uppercase=True
            )
            rows["con_code"] = encode_strings(
                frame["con_code"], CODE_BYTES, "con_code", uppercase=True
            )
            rows["snapshot_date"] = integer_dates(frame, "trade_date")
            weights = numeric(frame, "weight")
            if np.any(~np.isfinite(weights)) or np.any(
                (weights < 0.0) | (weights > 100.0)
            ):
                raise DataError("downloaded index_weight contains invalid weights")
            rows["weight"] = weights
            rows["source_order"] = self._source_orders("index_weight", len(frame))
        self._write_rows(
            partition,
            "index_weight",
            rows,
            code_field="index_code",
            bucket_count=INDEX_BUCKETS,
        )


class CsvUpdateStore:
    """Read business CSV partitions through bounded temporary CSV hash buckets."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)
        self.metadata_path = self.root / "cache-manifest.json"
        try:
            self.metadata = json.loads(self.metadata_path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise DataError(
                f"invalid CSV cache manifest: {self.metadata_path}"
            ) from exc
        if self.metadata.get("protocol") != CSV_CACHE_PROTOCOL:
            raise DataError(f"unsupported CSV cache protocol: {self.metadata_path}")
        if not self.metadata.get("complete"):
            raise DataError(f"CSV cache download is incomplete: {self.metadata_path}")
        partitions = self.metadata.get("partitions")
        if not isinstance(partitions, dict):
            raise DataError(f"invalid CSV cache partitions: {self.metadata_path}")
        self.partitions: dict[str, dict[str, Any]] = partitions
        self.target_dates = np.asarray(
            self.metadata.get("target_dates", []), dtype="<i4"
        )
        self.index_coverage = self.metadata.get("index_weight_coverage", {})
        if not isinstance(self.index_coverage, dict):
            raise DataError("CSV cache index weight coverage is invalid")
        self.build_root = self.root / ".build-buckets"
        shutil.rmtree(self.build_root, ignore_errors=True)
        self.build_root.mkdir(parents=True, exist_ok=False)
        self._prepared: dict[str, tuple[str, int]] = {}

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        shutil.rmtree(self.build_root, ignore_errors=True)

    def validate_partitions(self, *, full_hash: bool) -> None:
        for relative, entry in self.partitions.items():
            path = self.root / relative
            if not path.is_file() or path.stat().st_size != int(entry.get("size", -1)):
                raise DataError(f"CSV cache partition is missing or truncated: {path}")
            if full_hash and sha256_file(path) != str(entry.get("sha256", "")):
                raise DataError(f"CSV cache partition checksum failed: {path}")

    def _entries_for_key(self, key: str) -> list[tuple[Path, dict[str, Any]]]:
        return [
            (self.root / relative, entry)
            for relative, entry in sorted(self.partitions.items())
            if entry.get("key") == key
        ]

    def _prepare_key(self, key: str) -> tuple[str, int] | None:
        if key in self._prepared:
            return self._prepared[key]
        entries = self._entries_for_key(key)
        if not entries:
            return None
        code_fields = {str(entry.get("code_field")) for _, entry in entries}
        bucket_counts = {int(entry.get("bucket_count", 0)) for _, entry in entries}
        if len(code_fields) != 1 or len(bucket_counts) != 1:
            raise DataError(f"CSV cache bucket metadata is inconsistent: {key}")
        code_field = code_fields.pop()
        bucket_count = bucket_counts.pop()
        if not code_field or bucket_count <= 0:
            raise DataError(f"CSV cache bucket metadata is invalid: {key}")
        destination_root = self.build_root / Path(key)
        destination_root.mkdir(parents=True, exist_ok=True)
        initialized: set[int] = set()
        for source, entry in entries:
            if not source.is_file() or source.stat().st_size != int(
                entry.get("size", -1)
            ):
                raise DataError(
                    f"CSV cache partition is missing or truncated: {source}"
                )
            try:
                chunks = pd.read_csv(
                    source,
                    dtype=str,
                    keep_default_na=False,
                    chunksize=CSV_CHUNK_ROWS,
                )
                for chunk in chunks:
                    if code_field not in chunk:
                        raise DataError(
                            f"CSV cache code field {code_field!r} is missing: {source}"
                        )
                    codes = chunk[code_field].astype(str)
                    if codes.eq("").any():
                        raise DataError(f"CSV cache contains an empty code: {source}")
                    bucket_ids = np.fromiter(
                        (bucket_for(code, bucket_count) for code in codes),
                        dtype=np.int16,
                        count=len(chunk),
                    )
                    for bucket in np.unique(bucket_ids):
                        destination = destination_root / f"{int(bucket):03d}.csv"
                        chunk.loc[bucket_ids == bucket].to_csv(
                            destination,
                            mode="a",
                            header=int(bucket) not in initialized,
                            index=False,
                            encoding="utf-8",
                            lineterminator="\n",
                        )
                        initialized.add(int(bucket))
            except pd.errors.EmptyDataError:
                continue
        self._prepared[key] = (code_field, bucket_count)
        return self._prepared[key]

    def read_bucket(self, key: str, bucket: int, dtype: np.dtype) -> np.ndarray:
        metadata = self._prepare_key(key)
        if metadata is None:
            return np.empty(0, dtype=dtype)
        _, bucket_count = metadata
        if bucket < 0 or bucket >= bucket_count:
            raise DataError(f"CSV cache bucket is out of range: {key}/{bucket}")
        path = self.build_root / Path(key) / f"{bucket:03d}.csv"
        if not path.is_file():
            return np.empty(0, dtype=dtype)
        try:
            frame = pd.read_csv(path, dtype=str, keep_default_na=False)
        except pd.errors.EmptyDataError:
            return np.empty(0, dtype=dtype)
        return _frame_to_rows(frame, dtype, path)

    def daily_observations(
        self,
    ) -> dict[tuple[str, str], tuple[pd.Timestamp, pd.Timestamp]]:
        integer_bounds: dict[tuple[str, str], tuple[int, int]] = {}
        for bucket in range(DAILY_BUCKETS):
            rows = self.read_bucket("daily", bucket, DAILY_UPDATE_DTYPE)
            for row in rows:
                code = decode_code(row["ts_code"])
                asset_type = ASSET_TYPE_NAMES[int(row["asset_type"])]
                date = int(row["trade_date"])
                key = (code, asset_type)
                current = integer_bounds.get(key)
                integer_bounds[key] = (
                    date if current is None else min(current[0], date),
                    date if current is None else max(current[1], date),
                )

        def timestamp(value: int) -> pd.Timestamp:
            return pd.Timestamp(
                year=value // 10000,
                month=(value // 100) % 100,
                day=value % 100,
            )

        return {
            key: (timestamp(bounds[0]), timestamp(bounds[1]))
            for key, bounds in integer_bounds.items()
        }

    def categories(self, role: DailyRole) -> dict[str, set[str]]:
        result = {field: set() for field in role.categorical_fields}
        if not result:
            return result
        dtype = role_update_dtype(role)
        for bucket in range(DAILY_BUCKETS):
            rows = self.read_bucket(role.role, bucket, dtype)
            for field in role.categorical_fields:
                result[field].update(
                    value
                    for value in (decode_category(item) for item in rows[field])
                    if value
                )
        return result
