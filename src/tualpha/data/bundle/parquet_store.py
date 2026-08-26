"""Parquet Bundle metadata, catalog, validation, and atomic file helpers."""

from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from ...exceptions import DataError
from .parquet_schema import TABLE_SPECS, TableSpec

BUNDLE_PROTOCOL = "tualpha.parquet/1"
BUNDLE_SCHEMA_VERSION = 1
MANIFEST_FILE = "manifest.json"
CATALOG_FILE = "catalog.duckdb"
PARQUET_DIRECTORY = "parquet"

_ARROW_TYPES: dict[str, pa.DataType] = {
    "VARCHAR": pa.string(),
    "DOUBLE": pa.float64(),
    "UTINYINT": pa.uint8(),
    "UBIGINT": pa.uint64(),
}


def sha256_file(path: str | Path, *, chunk_size: int = 8 * 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        while chunk := stream.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    os.replace(temporary, path)


def load_manifest(bundle_path: str | Path) -> dict[str, Any]:
    source = Path(bundle_path) / MANIFEST_FILE
    try:
        payload = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise DataError(f"Parquet Bundle manifest is invalid: {source}") from exc
    if payload.get("protocol") != BUNDLE_PROTOCOL:
        raise DataError(f"unsupported Bundle protocol: {payload.get('protocol')!r}")
    if int(payload.get("schema_version", -1)) != BUNDLE_SCHEMA_VERSION:
        raise DataError(f"unsupported Bundle schema: {payload.get('schema_version')!r}")
    generation = payload.get("generation")
    if not isinstance(generation, str) or not generation:
        raise DataError("Bundle generation is missing")
    if not isinstance(payload.get("files"), dict):
        raise DataError("Bundle file metadata is missing")
    return payload


def arrow_schema(spec: TableSpec) -> pa.Schema:
    return pa.schema(
        [(name, _ARROW_TYPES[data_type]) for name, data_type in spec.columns]
    )


def normalize_frame(frame: pd.DataFrame, spec: TableSpec) -> pd.DataFrame:
    result = frame.copy()
    for name, data_type in spec.columns:
        if name not in result:
            result[name] = None
        if data_type == "VARCHAR":
            values = result[name].astype("string").str.strip()
            result[name] = values.mask(values.eq(""), pd.NA)
        elif data_type == "DOUBLE":
            result[name] = pd.to_numeric(result[name], errors="coerce").astype(float)
        elif data_type == "UTINYINT":
            result[name] = (
                pd.to_numeric(result[name], errors="coerce").fillna(0).astype("uint8")
            )
        elif data_type == "UBIGINT":
            result[name] = (
                pd.to_numeric(result[name], errors="coerce").fillna(0).astype("uint64")
            )
    return result.loc[:, list(spec.column_names)]


def write_parquet_atomic(
    frame: pd.DataFrame,
    destination: str | Path,
    spec: TableSpec,
    *,
    row_group_size: int = 122_880,
) -> Path:
    path = Path(destination)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    source = frame.copy()
    for column in spec.column_names:
        if column not in source:
            source[column] = None
    connection = duckdb.connect()
    try:
        connection.register("input_frame", source)
        expressions = ", ".join(
            f'TRY_CAST("{name}" AS {data_type}) AS "{name}"'
            for name, data_type in spec.columns
        )
        destination_sql = str(temporary.resolve()).replace("\\", "/").replace("'", "''")
        connection.execute(
            f"COPY (SELECT {expressions} FROM input_frame) TO '{destination_sql}' "
            f"(FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 6, "
            f"ROW_GROUP_SIZE {int(row_group_size)}, PRESERVE_ORDER)"
        )
    except duckdb.Error as exc:
        raise DataError(f"failed to write Parquet table {spec.name}: {exc}") from exc
    finally:
        connection.close()
    os.replace(temporary, path)
    return path


def parquet_relative_path(
    spec: TableSpec,
    *,
    year: int | str | None = None,
    index_code: str | None = None,
    exchange: str | None = None,
) -> Path:
    base = Path(PARQUET_DIRECTORY) / spec.path
    if spec.partition_column == "year":
        if year is None:
            raise ValueError(f"year is required for {spec.name}")
        return base / f"year={year}" / "data.parquet"
    if spec.partition_column == "report_year":
        if year is None:
            raise ValueError(f"report year is required for {spec.name}")
        return base / f"report_year={year}" / "data.parquet"
    if spec.partition_column == "index_code_year":
        if year is None or not index_code:
            raise ValueError("index_code and year are required for index_weight")
        return base / f"index_code={index_code}" / f"year={year}" / "data.parquet"
    if spec.partition_column == "exchange":
        if not exchange:
            raise ValueError("exchange is required for trade_cal")
        return base / f"exchange={exchange}" / "data.parquet"
    return base / "data.parquet"


def build_catalog(
    bundle_path: str | Path,
    *,
    generation: str,
    assets: pd.DataFrame,
    trade_calendar: pd.DataFrame,
    datasets: pd.DataFrame,
) -> Path:
    root = Path(bundle_path)
    destination = root / CATALOG_FILE
    temporary = root / f".{CATALOG_FILE}.tmp"
    temporary.unlink(missing_ok=True)
    connection = duckdb.connect(str(temporary))
    try:
        connection.execute(
            "CREATE TABLE bundle_metadata(key VARCHAR PRIMARY KEY, value VARCHAR NOT NULL)"
        )
        connection.executemany(
            "INSERT INTO bundle_metadata VALUES (?, ?)",
            [
                ("protocol", BUNDLE_PROTOCOL),
                ("schema_version", str(BUNDLE_SCHEMA_VERSION)),
                ("generation", generation),
            ],
        )
        connection.register("assets_frame", assets)
        connection.execute("CREATE TABLE assets AS SELECT * FROM assets_frame")
        connection.execute("CREATE UNIQUE INDEX assets_sid_idx ON assets(sid)")
        connection.execute("CREATE UNIQUE INDEX assets_code_idx ON assets(ts_code)")
        connection.register("calendar_frame", trade_calendar)
        connection.execute(
            "CREATE TABLE trade_calendar AS SELECT * FROM calendar_frame"
        )
        connection.execute(
            "CREATE UNIQUE INDEX calendar_idx ON trade_calendar(exchange, cal_date)"
        )
        connection.register("datasets_frame", datasets)
        connection.execute("CREATE TABLE datasets AS SELECT * FROM datasets_frame")
        connection.execute("CHECKPOINT")
    finally:
        connection.close()
    os.replace(temporary, destination)
    return destination


def catalog_generation(bundle_path: str | Path) -> str:
    path = Path(bundle_path) / CATALOG_FILE
    try:
        connection = duckdb.connect(str(path), read_only=True)
    except duckdb.Error as exc:
        raise DataError(f"DuckDB catalog is invalid: {path}") from exc
    try:
        row = connection.execute(
            "SELECT value FROM bundle_metadata WHERE key='generation'"
        ).fetchone()
    except duckdb.Error as exc:
        raise DataError(f"DuckDB catalog metadata is invalid: {path}") from exc
    finally:
        connection.close()
    if row is None or not row[0]:
        raise DataError("DuckDB catalog generation is missing")
    return str(row[0])


def collect_file_metadata(
    bundle_path: str | Path,
    previous_files: dict[str, dict[str, Any]] | None = None,
) -> dict[str, dict[str, Any]]:
    root = Path(bundle_path)
    inherited = previous_files or {}
    files: dict[str, dict[str, Any]] = {}
    for path in sorted((root / PARQUET_DIRECTORY).rglob("*.parquet")):
        relative = path.relative_to(root).as_posix()
        previous = inherited.get(relative)
        if (
            isinstance(previous, dict)
            and int(previous.get("size", -1)) == path.stat().st_size
        ):
            files[relative] = dict(previous)
            continue
        metadata = pq.ParquetFile(path).metadata
        files[relative] = {
            "rows": int(metadata.num_rows),
            "row_groups": int(metadata.num_row_groups),
            "size": path.stat().st_size,
            "sha256": sha256_file(path),
        }
    catalog = root / CATALOG_FILE
    if catalog.is_file():
        files[CATALOG_FILE] = {
            "size": catalog.stat().st_size,
            "sha256": sha256_file(catalog),
        }
    return files


def create_manifest(
    bundle_path: str | Path,
    *,
    generation: str,
    start_session: str,
    end_session: str,
    session_count: int,
    asset_count: int,
    index_count: int,
    build_pipeline: str,
    previous_files: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    root = Path(bundle_path)
    files = collect_file_metadata(root, previous_files)
    manifest = {
        "protocol": BUNDLE_PROTOCOL,
        "schema_version": BUNDLE_SCHEMA_VERSION,
        "generation": generation,
        "generated_at": datetime.now(ZoneInfo("UTC")).isoformat(),
        "start_session": str(start_session),
        "end_session": str(end_session),
        "session_count": int(session_count),
        "asset_count": int(asset_count),
        "index_count": int(index_count),
        "calendar_source": "tushare.trade_cal:SSE",
        "build_pipeline": build_pipeline,
        "partitioning": "year/v1",
        "files": files,
        "tables": {
            name: {
                "path": spec.path,
                "glob": spec.parquet_glob,
                "primary_key": list(spec.primary_key),
                "date_column": spec.date_column,
                "partition_column": spec.partition_column,
                "columns": {column: data_type for column, data_type in spec.columns},
            }
            for name, spec in TABLE_SPECS.items()
        },
        "pit_rules": {
            "finance": "effective_ann_date < session AND end_date <= session",
            "index_weight": "max(trade_date) < session",
        },
    }
    write_json_atomic(root / MANIFEST_FILE, manifest)
    return manifest


def validate_bundle(
    bundle_path: str | Path, *, full_hash: bool = False
) -> dict[str, Any]:
    root = Path(bundle_path)
    if not root.is_dir():
        raise DataError(f"Parquet Bundle directory does not exist: {root}")
    manifest = load_manifest(root)
    if catalog_generation(root) != str(manifest["generation"]):
        raise DataError("Bundle manifest and DuckDB catalog generations differ")
    parquet_root = root / PARQUET_DIRECTORY
    if not parquet_root.is_dir():
        raise DataError("Bundle parquet directory is missing")
    expected = set(manifest["files"])
    actual = {
        path.relative_to(root).as_posix() for path in parquet_root.rglob("*.parquet")
    }
    actual.add(CATALOG_FILE) if (root / CATALOG_FILE).is_file() else None
    if actual != expected:
        raise DataError(
            f"Bundle file set is invalid; missing={sorted(expected - actual)}, extra={sorted(actual - expected)}"
        )
    for relative, entry in manifest["files"].items():
        path = root / relative
        if not path.is_file() or path.stat().st_size != int(entry.get("size", -1)):
            raise DataError(f"Bundle file size does not match: {relative}")
        if full_hash and sha256_file(path) != str(entry.get("sha256", "")):
            raise DataError(f"Bundle file hash does not match: {relative}")
        if path.suffix == ".parquet":
            try:
                metadata = pq.ParquetFile(path).metadata
            except Exception as exc:
                raise DataError(f"invalid Parquet file: {relative}") from exc
            if int(entry.get("rows", -1)) != metadata.num_rows:
                raise DataError(f"Parquet row count does not match: {relative}")
    return manifest
