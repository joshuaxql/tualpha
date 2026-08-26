"""Build DuckDB catalog inputs from canonical Parquet datasets."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import pyarrow.parquet as pq

from .parquet_schema import (
    ETF_DAILY,
    INDEX_DAILY,
    STOCK_DAILY,
    TABLE_SPECS,
)
from .parquet_store import CATALOG_FILE, PARQUET_DIRECTORY


def _sql_path(path: Path) -> str:
    return str(path.resolve()).replace("\\", "/").replace("'", "''")


def _existing_sid_map(active_bundle: Path) -> dict[str, int]:
    catalog = active_bundle / CATALOG_FILE
    if catalog.is_file():
        connection = duckdb.connect(str(catalog), read_only=True)
        try:
            return {
                str(code).upper(): int(sid)
                for code, sid in connection.execute(
                    "SELECT ts_code, sid FROM assets"
                ).fetchall()
            }
        finally:
            connection.close()
    source = active_bundle / "assets.pk"
    if not source.is_file():
        return {}
    try:
        from .legacy_manifest import load_legacy_assets_manifest

        manifest = load_legacy_assets_manifest(source)
        return {
            str(row["ts_code"]).upper(): int(row["sid"])
            for row in manifest.get("assets", [])
        }
    except Exception:  # noqa: BLE001 - legacy SID preservation is best effort
        return {}


def build_asset_table(
    active_bundle: Path,
    stock: pd.DataFrame,
    etf: pd.DataFrame,
    indices: pd.DataFrame,
    observations: pd.DataFrame,
    start_session: str,
    end_session: str,
) -> pd.DataFrame:
    bounds = {
        (str(row.asset_type), str(row.ts_code).upper()): (
            str(row.first_date),
            str(row.last_date),
        )
        for row in observations.itertuples(index=False)
    }
    previous = _existing_sid_map(active_bundle)
    next_sid = max(previous.values(), default=0) + 1

    def sid(code: str) -> int:
        nonlocal next_sid
        if code not in previous:
            previous[code] = next_sid
            next_sid += 1
        return previous[code]

    records: list[dict[str, object]] = []
    exchange_map = {
        "SSE": "SSE",
        "SZSE": "SZSE",
        "BSE": "BSE",
        "SH": "SSE",
        "SZ": "SZSE",
    }
    board_map = {
        "主板": "main",
        "创业板": "chinext",
        "科创板": "star",
        "北交所": "bse",
    }

    for row in stock.to_dict("records"):
        code = str(row.get("ts_code", "")).upper()
        observed = bounds.get(("stock", code))
        if not code or observed is None:
            continue
        list_date = str(row.get("list_date") or observed[0])
        delist_date = str(row.get("delist_date") or end_session)
        first = max(start_session, list_date, observed[0])
        last = min(end_session, delist_date)
        if first > last:
            continue
        board = board_map.get(str(row.get("market", "")), "unknown")
        records.append(
            {
                "sid": sid(code),
                "ts_code": code,
                "symbol": code.split(".")[0],
                "name": str(row.get("name") or code),
                "asset_type": "stock",
                "tradable": True,
                "exchange": exchange_map.get(
                    str(row.get("exchange", "")), code.rsplit(".", 1)[-1]
                ),
                "board": board,
                "list_date": first,
                "delist_date": last,
                "price_tick": 0.01,
                "round_lot": 1 if board in {"star", "bse"} else 100,
                "minimum_order": 200 if board == "star" else 100,
                "settlement_days": 1,
            }
        )

    for code, row in {
        str(item.get("ts_code", "")).upper(): item for item in etf.to_dict("records")
    }.items():
        observed = bounds.get(("etf", code))
        if not code or observed is None:
            continue
        list_date = str(row.get("list_date") or observed[0])
        first = max(start_session, list_date, observed[0])
        last = end_session if str(row.get("list_status", "")) == "L" else observed[1]
        last = min(end_session, last)
        if first > last:
            continue
        records.append(
            {
                "sid": sid(code),
                "ts_code": code,
                "symbol": code.split(".")[0],
                "name": str(row.get("extname") or row.get("csname") or code),
                "asset_type": "etf",
                "tradable": True,
                "exchange": exchange_map.get(
                    str(row.get("exchange", "")), code.rsplit(".", 1)[-1]
                ),
                "board": "etf",
                "list_date": first,
                "delist_date": last,
                "price_tick": 0.001,
                "round_lot": 100,
                "minimum_order": 100,
                "settlement_days": 1,
            }
        )

    for row in indices.to_dict("records"):
        code = str(row.get("ts_code", "")).upper()
        if not code:
            continue
        observed = bounds.get(("index", code))
        list_date = str(
            row.get("list_date") or (observed[0] if observed else start_session)
        )
        exp_date = str(
            row.get("exp_date") or (observed[1] if observed else end_session)
        )
        records.append(
            {
                "sid": sid(code),
                "ts_code": code,
                "symbol": code.split(".")[0],
                "name": str(row.get("name") or code),
                "asset_type": "index",
                "tradable": False,
                "exchange": str(row.get("market") or code.rsplit(".", 1)[-1]),
                "board": "index",
                "list_date": max(start_session, list_date),
                "delist_date": min(end_session, exp_date),
                "price_tick": 0.01,
                "round_lot": 0,
                "minimum_order": 0,
                "settlement_days": 0,
            }
        )
    return (
        pd.DataFrame(records).sort_values("sid", kind="stable").reset_index(drop=True)
    )


def daily_observations(bundle_path: Path) -> pd.DataFrame:
    connection = duckdb.connect()
    selects = []
    for asset_type, spec in (
        ("stock", STOCK_DAILY),
        ("etf", ETF_DAILY),
        ("index", INDEX_DAILY),
    ):
        glob = bundle_path / PARQUET_DIRECTORY / spec.parquet_glob
        selects.append(
            f"SELECT '{asset_type}' asset_type, ts_code, "
            "min(trade_date) first_date, max(trade_date) last_date "
            f"FROM read_parquet('{_sql_path(glob)}', hive_partitioning=true) "
            "GROUP BY ts_code"
        )
    try:
        return connection.execute(" UNION ALL ".join(selects)).fetchdf()
    finally:
        connection.close()


def datasets_frame(bundle_path: Path) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for name, spec in TABLE_SPECS.items():
        files = sorted((bundle_path / PARQUET_DIRECTORY / spec.path).rglob("*.parquet"))
        count = sum(pq.ParquetFile(path).metadata.num_rows for path in files)
        rows.append(
            {
                "table_name": name,
                "path": spec.path,
                "parquet_glob": spec.parquet_glob,
                "row_count": int(count),
                "partition_count": len(files),
                "date_column": spec.date_column,
            }
        )
    return pd.DataFrame(rows)
