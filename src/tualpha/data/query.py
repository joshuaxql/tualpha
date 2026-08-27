"""DuckDB-backed local queries over the immutable Parquet Bundle."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Self

import duckdb
import pandas as pd

from ..foundation.config import DEFAULT_BUNDLE_ROOT
from ..foundation.exceptions import DataError
from .bundle.manager import (
    BUNDLE_NAME,
    acquire_bundle_read_lock,
    latest_bundle_path,
    release_bundle_read_lock,
)
from .bundle.parquet_schema import TABLE_SPECS, TableSpec
from .bundle.parquet_store import CATALOG_FILE, load_manifest

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


def _quote_path(path: Path) -> str:
    return str(path.resolve()).replace("\\", "/").replace("'", "''")


def _quote_identifier(value: str) -> str:
    if not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"invalid SQL identifier: {value!r}")
    return f'"{value}"'


def parquet_scan_sql(bundle_path: Path, spec: TableSpec) -> str:
    pattern = bundle_path / "parquet" / spec.parquet_glob
    return f"read_parquet('{_quote_path(pattern)}', hive_partitioning=true)"


class LocalDataClient:
    """One read-locked DuckDB session exposing registered Parquet tables."""

    def __init__(
        self,
        bundle_root: str | Path = DEFAULT_BUNDLE_ROOT,
        bundle_name: str = BUNDLE_NAME,
    ) -> None:
        self.bundle_root = Path(bundle_root).expanduser()
        self.bundle_name = bundle_name
        self._lock_key, _ = acquire_bundle_read_lock(self.bundle_root, bundle_name)
        try:
            self.bundle_path = latest_bundle_path(self.bundle_root, bundle_name)
            self.manifest = load_manifest(self.bundle_path)
            self.connection = duckdb.connect(":memory:")
            self.connection.execute(
                f"ATTACH '{_quote_path(self.bundle_path / CATALOG_FILE)}' AS catalog (READ_ONLY)"
            )
            for name, spec in TABLE_SPECS.items():
                source = parquet_scan_sql(self.bundle_path, spec)
                self.connection.execute(
                    f"CREATE VIEW {_quote_identifier(name)} AS SELECT * FROM {source}"
                )
        except Exception:
            release_bundle_read_lock(self._lock_key)
            self._lock_key = None
            raise

    def close(self) -> None:
        connection = getattr(self, "connection", None)
        if connection is not None:
            connection.close()
            self.connection = None
        if getattr(self, "_lock_key", None) is not None:
            release_bundle_read_lock(self._lock_key)
            self._lock_key = None

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()

    def sql(self, query: str, params: Sequence[object] = ()) -> pd.DataFrame:
        """Run read-only SQL against Parquet views and ``catalog.*`` metadata."""

        statement = query.lstrip().split(None, 1)[0].upper() if query.strip() else ""
        if statement not in {
            "SELECT",
            "WITH",
            "EXPLAIN",
            "DESCRIBE",
            "SHOW",
            "SUMMARIZE",
        }:
            raise ValueError("local SQL must be read-only")
        try:
            return self.connection.execute(query, list(params)).fetchdf()
        except duckdb.Error as exc:
            raise DataError(f"local DuckDB query failed: {exc}") from exc

    def query(
        self,
        table: str,
        *,
        fields: str | Sequence[str] | None = None,
        filters: Mapping[str, object] | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        limit: int | None = None,
    ) -> pd.DataFrame:
        try:
            spec = TABLE_SPECS[table]
        except KeyError as exc:
            raise KeyError(f"unknown local table: {table!r}") from exc
        selected = (
            [value.strip() for value in fields.split(",") if value.strip()]
            if isinstance(fields, str)
            else list(fields)
            if fields is not None
            else list(spec.column_names)
        )
        unknown = set(selected).difference(spec.column_names)
        if unknown:
            raise ValueError(f"unknown fields for {table}: {sorted(unknown)}")
        clauses: list[str] = []
        params: list[object] = []
        for column, value in (filters or {}).items():
            if column not in spec.column_names:
                raise ValueError(f"unknown filter column for {table}: {column}")
            if isinstance(value, (list, tuple, set, frozenset)):
                items = list(value)
                if not items:
                    return pd.DataFrame(columns=selected)
                clauses.append(
                    f"{_quote_identifier(column)} IN ({', '.join('?' for _ in items)})"
                )
                params.extend(items)
            else:
                clauses.append(f"{_quote_identifier(column)} = ?")
                params.append(value)
        if start_date is not None or end_date is not None:
            if spec.date_column is None:
                raise ValueError(f"{table} has no date column")
            date_column = _quote_identifier(spec.date_column)
            if start_date is not None:
                clauses.append(f"{date_column} >= ?")
                params.append(str(start_date))
            if end_date is not None:
                clauses.append(f"{date_column} <= ?")
                params.append(str(end_date))
        sql = (
            f"SELECT {', '.join(_quote_identifier(value) for value in selected)} "
            f"FROM {_quote_identifier(table)}"
        )
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        if spec.date_column:
            sql += f" ORDER BY {_quote_identifier(spec.date_column)}"
            if "ts_code" in spec.column_names:
                sql += ", ts_code"
        elif "ts_code" in spec.column_names:
            sql += " ORDER BY ts_code"
        if limit is not None:
            if limit < 0:
                raise ValueError("limit must not be negative")
            sql += " LIMIT ?"
            params.append(limit)
        return self.sql(sql, params)


def local_data(
    bundle_root: str | Path = DEFAULT_BUNDLE_ROOT,
    bundle_name: str = BUNDLE_NAME,
) -> LocalDataClient:
    return LocalDataClient(bundle_root, bundle_name)
