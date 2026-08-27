"""Apply downloaded CSV batches to immutable yearly Parquet generations."""

from __future__ import annotations

import json
import os
import shutil
from collections.abc import Sequence
from contextlib import suppress
from pathlib import Path
from uuid import uuid4

import duckdb
import pandas as pd
from filelock import FileLock

from ...foundation.exceptions import DataError
from .catalog_builder import build_asset_table, daily_observations, datasets_frame
from .manager import BUNDLE_NAME, bundle_lock_path, bundle_path, publish_fixed_bundle
from .parquet_schema import (
    ADJ_FACTOR,
    DAILY_BASIC,
    ETF_ADJ_FACTOR,
    ETF_BASIC,
    ETF_DAILY,
    FINANCE_SPECS,
    INDEX_BASIC,
    INDEX_DAILY,
    INDEX_DAILY_CODES,
    INDEX_WEIGHT,
    INDUSTRY,
    MONEYFLOW,
    STK_LIMIT,
    STOCK_BASIC,
    STOCK_DAILY,
    STOCK_ST,
    SUSPEND_D,
    TRADE_CAL,
    TableSpec,
)
from .parquet_store import (
    CATALOG_FILE,
    MANIFEST_FILE,
    PARQUET_DIRECTORY,
    build_catalog,
    create_manifest,
    load_manifest,
    normalize_frame,
    parquet_relative_path,
    validate_bundle,
    write_parquet_atomic,
)
from .registry import BundleBuildResult

DAILY_DATASET_BY_TABLE = {
    STOCK_DAILY.name: "daily",
    ETF_DAILY.name: "fund_daily",
    INDEX_DAILY.name: "index_daily",
    ADJ_FACTOR.name: "adj_factor",
    ETF_ADJ_FACTOR.name: "fund_adj",
    DAILY_BASIC.name: "daily_basic",
    MONEYFLOW.name: "moneyflow",
    STK_LIMIT.name: "stk_limit",
    SUSPEND_D.name: "suspend_d",
    STOCK_ST.name: "stock_st",
    INDUSTRY.name: "industry",
}


class ParquetBuild:
    def __init__(self, result: BundleBuildResult, manifest: dict[str, object]):
        self.result = result
        self.manifest = manifest


def find_active_bundle(
    bundle_root: str | Path, bundle_name: str = BUNDLE_NAME
) -> Path | None:
    path = bundle_path(bundle_root, bundle_name)
    if not (path / MANIFEST_FILE).is_file():
        return None
    validate_bundle(path, full_hash=False)
    return path


def active_trade_dates(
    bundle_root: str | Path, bundle_name: str = BUNDLE_NAME
) -> pd.DatetimeIndex:
    active = find_active_bundle(bundle_root, bundle_name)
    if active is None:
        return pd.DatetimeIndex([])
    connection = duckdb.connect(str(active / CATALOG_FILE), read_only=True)
    try:
        rows = connection.execute(
            "SELECT cal_date FROM trade_calendar WHERE exchange='SSE' AND CAST(is_open AS INTEGER)=1 ORDER BY cal_date"
        ).fetchall()
    finally:
        connection.close()
    return pd.DatetimeIndex(
        pd.to_datetime([str(row[0]) for row in rows], format="%Y%m%d")
    )


def active_index_weight_state(
    bundle_root: str | Path, bundle_name: str = BUNDLE_NAME
) -> tuple[dict[str, set[str]], dict[str, object]]:
    active = find_active_bundle(bundle_root, bundle_name)
    if active is None:
        return {}, {}
    pattern = active / PARQUET_DIRECTORY / INDEX_WEIGHT.parquet_glob
    connection = duckdb.connect()
    try:
        rows = connection.execute(
            "SELECT index_code, trade_date FROM read_parquet(?, hive_partitioning=true) GROUP BY index_code, trade_date",
            [str(pattern)],
        ).fetchall()
    finally:
        connection.close()
    result: dict[str, set[str]] = {}
    for code, date in rows:
        result.setdefault(str(code).upper(), set()).add(str(date))
    coverage = {
        "schema_version": 1,
        "codes": {
            code: {"from": min(dates), "through": max(dates)}
            for code, dates in result.items()
            if dates
        },
    }
    return result, coverage


def _hardlink_or_copy(source: str, destination: str) -> str:
    try:
        os.link(source, destination)
    except OSError:
        shutil.copy2(source, destination)
    return destination


def _cache_frames(cache_path: Path, key: str) -> pd.DataFrame:
    metadata_path = cache_path / "cache-manifest.json"
    try:
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise DataError(f"invalid update cache manifest: {metadata_path}") from exc
    frames: list[pd.DataFrame] = []
    for relative, entry in sorted(metadata.get("partitions", {}).items()):
        if entry.get("key") != key:
            continue
        path = cache_path / relative
        try:
            # Pandas 3 infers Arrow-backed ``str`` columns by default. CSV cache
            # frames are mutable staging data, so keep explicit object columns and
            # normalize them only at the Parquet schema boundary.
            frames.append(pd.read_csv(path, dtype=object, keep_default_na=False))
        except pd.errors.EmptyDataError:
            continue
    return (
        pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame()
    )


def _read_existing(path: Path, spec: TableSpec) -> pd.DataFrame:
    if not path.is_file():
        return pd.DataFrame(columns=spec.column_names)
    return pd.read_parquet(path)


def _deduplicate(frame: pd.DataFrame, spec: TableSpec) -> pd.DataFrame:
    result = frame.copy()
    for column in spec.column_names:
        if column not in result:
            result[column] = None
    result = result.loc[:, list(spec.column_names)]
    if result.empty:
        return result
    result = result.drop_duplicates(list(spec.primary_key), keep="last")
    order = list(
        dict.fromkeys(
            ([spec.date_column] if spec.date_column else []) + list(spec.primary_key)
        )
    )
    return result.sort_values(order, kind="stable").reset_index(drop=True)


def _daily_incoming(
    cache_path: Path,
    spec: TableSpec,
    etf_codes: set[str],
    index_daily_codes: set[str],
) -> pd.DataFrame:
    if spec in {STOCK_DAILY, ETF_DAILY, INDEX_DAILY}:
        frame = _cache_frames(cache_path, "daily")
        if frame.empty:
            return frame
        asset_id = {STOCK_DAILY: "0", ETF_DAILY: "1", INDEX_DAILY: "2"}[spec]
        selected = frame[frame["asset_type"].astype(str) == asset_id].copy()
        if spec is ETF_DAILY:
            selected = selected[
                selected["ts_code"].astype(str).str.upper().isin(etf_codes)
            ]
        elif spec is INDEX_DAILY:
            selected = selected[
                selected["ts_code"].astype(str).str.upper().isin(index_daily_codes)
            ]
        return selected
    if spec in {ADJ_FACTOR, ETF_ADJ_FACTOR}:
        frame = _cache_frames(cache_path, "adj_factor")
        if frame.empty:
            return frame
        is_etf = frame["ts_code"].astype(str).str.upper().isin(etf_codes)
        return frame[is_etf if spec is ETF_ADJ_FACTOR else ~is_etf].copy()
    key = spec.name
    frame = _cache_frames(cache_path, key)
    if frame.empty:
        return frame
    if spec is SUSPEND_D:
        return pd.DataFrame(
            {
                "trade_date": frame["trade_date"],
                "ts_code": frame["ts_code"],
                "suspend_timing": None,
                "suspend_type": "S",
            }
        )
    if spec is STOCK_ST:
        return frame.drop(columns=["is_st", "source_order"], errors="ignore")
    return frame.drop(columns=["source_order"], errors="ignore")


def _replace_daily_year(
    staged: Path,
    spec: TableSpec,
    year: str,
    target_dates: set[str],
    incoming: pd.DataFrame,
    changed: set[str],
) -> None:
    relative = parquet_relative_path(spec, year=year)
    path = staged / relative
    active = _read_existing(path, spec)
    if not active.empty:
        active = active[~active[spec.date_column].astype(str).isin(target_dates)]
    selected = (
        incoming[incoming[spec.date_column].astype(str).str.startswith(year)]
        if not incoming.empty
        else incoming
    )
    merged = _deduplicate(
        pd.concat([active, selected], ignore_index=True, sort=False), spec
    )
    write_parquet_atomic(merged, path, spec)
    changed.add(relative.as_posix())


def _merge_finance(staged: Path, cache_path: Path, changed: set[str]) -> None:
    for table, spec in FINANCE_SPECS.items():
        incoming = _cache_frames(cache_path, f"finance/{table}")
        if incoming.empty:
            continue
        years = sorted({str(value)[:4] for value in incoming["end_date"] if str(value)})
        for year in years:
            relative = parquet_relative_path(spec, year=year)
            path = staged / relative
            active = _read_existing(path, spec)
            selected = incoming[
                incoming["end_date"].astype(str).str.startswith(year)
            ].copy()
            merged = _deduplicate(
                pd.concat([active, selected], ignore_index=True, sort=False), spec
            )
            write_parquet_atomic(merged, path, spec)
            changed.add(relative.as_posix())


def _merge_index_weights(staged: Path, cache_path: Path, changed: set[str]) -> None:
    incoming = _cache_frames(cache_path, "index_weight")
    if incoming.empty:
        return
    incoming = incoming.rename(columns={"snapshot_date": "trade_date"})
    incoming = incoming.drop(columns=["source_order"], errors="ignore")
    for (code, year), selected in incoming.groupby(
        [
            incoming["index_code"].astype(str).str.upper(),
            incoming["trade_date"].astype(str).str[:4],
        ],
        sort=True,
    ):
        relative = parquet_relative_path(INDEX_WEIGHT, year=year, index_code=code)
        path = staged / relative
        active = _read_existing(path, INDEX_WEIGHT)
        snapshots = set(selected["trade_date"].astype(str))
        if not active.empty:
            active = active[~active["trade_date"].astype(str).isin(snapshots)]
        merged = _deduplicate(
            pd.concat([active, selected], ignore_index=True, sort=False), INDEX_WEIGHT
        )
        write_parquet_atomic(merged, path, INDEX_WEIGHT)
        changed.add(relative.as_posix())


def _stored_index_daily_codes(staged: Path) -> set[str]:
    directory = staged / PARQUET_DIRECTORY / INDEX_DAILY.path
    paths = sorted(directory.glob("year=*/data.parquet"))
    if not paths:
        return set()
    connection = duckdb.connect()
    try:
        rows = connection.execute(
            "SELECT DISTINCT upper(ts_code) FROM read_parquet(?, "
            "hive_partitioning=true)",
            [[str(path) for path in paths]],
        ).fetchall()
    finally:
        connection.close()
    return {str(row[0]) for row in rows}


def _sql_code_list(codes: Sequence[str]) -> str:
    return ", ".join(f"'{code.replace(chr(39), chr(39) * 2)}'" for code in codes)


def _prune_index_daily(
    staged: Path, changed: set[str], index_daily_codes: Sequence[str]
) -> None:
    directory = staged / PARQUET_DIRECTORY / INDEX_DAILY.path
    allowed = _sql_code_list(index_daily_codes)
    columns = ", ".join(f'"{column}"' for column in INDEX_DAILY.column_names)
    for path in sorted(directory.glob("year=*/data.parquet")):
        connection = duckdb.connect()
        temporary = path.with_name(f".{path.name}.tmp")
        temporary.unlink(missing_ok=True)
        source = str(path.resolve()).replace("\\", "/").replace("'", "''")
        destination = str(temporary.resolve()).replace("\\", "/").replace("'", "''")
        try:
            unexpected = connection.execute(
                f"SELECT count(*) FROM read_parquet('{source}') "
                f"WHERE upper(ts_code) NOT IN ({allowed})"
            ).fetchone()[0]
            if not unexpected:
                continue
            connection.execute(
                f"COPY (SELECT {columns} FROM read_parquet('{source}') "
                f"WHERE upper(ts_code) IN ({allowed}) ORDER BY trade_date, ts_code) "
                f"TO '{destination}' (FORMAT PARQUET, COMPRESSION ZSTD, "
                "COMPRESSION_LEVEL 6, ROW_GROUP_SIZE 122880, PRESERVE_ORDER)"
            )
        except duckdb.Error as exc:
            raise DataError(
                f"failed to restrict index_daily file {path}: {exc}"
            ) from exc
        finally:
            connection.close()
        os.replace(temporary, path)
        changed.add(path.relative_to(staged).as_posix())


def _validate_critical_data(staged: Path, index_daily_codes: Sequence[str]) -> None:
    pattern = staged / PARQUET_DIRECTORY / INDEX_WEIGHT.parquet_glob
    index_daily_pattern = staged / PARQUET_DIRECTORY / INDEX_DAILY.parquet_glob
    allowed = _sql_code_list(index_daily_codes)
    connection = duckdb.connect()
    try:
        unexpected_indices = connection.execute(
            "SELECT count(DISTINCT ts_code) FROM read_parquet(?, "
            "hive_partitioning=true) "
            f"WHERE upper(ts_code) NOT IN ({allowed})",
            [str(index_daily_pattern)],
        ).fetchone()[0]
        invalid = connection.execute(
            "SELECT count(*) FROM read_parquet(?, hive_partitioning=true) "
            "WHERE weight IS NULL OR weight < 0 OR weight > 100",
            [str(pattern)],
        ).fetchone()[0]
        duplicates = connection.execute(
            "SELECT count(*) FROM (SELECT index_code, trade_date, con_code, "
            "count(*) AS n FROM read_parquet(?, hive_partitioning=true) "
            "GROUP BY index_code, trade_date, con_code HAVING n > 1)",
            [str(pattern)],
        ).fetchone()[0]
    finally:
        connection.close()
    if unexpected_indices:
        raise DataError(
            f"updated index_daily contains {unexpected_indices} unsupported indices"
        )
    if invalid:
        raise DataError(f"updated index_weight contains {invalid} invalid weights")
    if duplicates:
        raise DataError(
            f"updated index_weight contains {duplicates} duplicate constituent keys"
        )


def _merge_requested_index_daily(
    staged: Path, requested: pd.DataFrame, changed: set[str]
) -> None:
    if requested.empty:
        return
    incoming = requested.rename(columns={"vol": "volume", "amount": "turnover"})
    incoming = incoming.copy()
    incoming["turnover"] = pd.to_numeric(incoming["turnover"], errors="coerce") * 1000.0
    incoming = normalize_frame(incoming, INDEX_DAILY)
    for year, selected in incoming.groupby(
        incoming["trade_date"].astype(str).str[:4], sort=True
    ):
        relative = parquet_relative_path(INDEX_DAILY, year=str(year))
        path = staged / relative
        active = _read_existing(path, INDEX_DAILY)
        if not active.empty:
            remove = pd.Series(False, index=active.index)
            active_codes = active["ts_code"].astype(str).str.upper()
            active_dates = active["trade_date"].astype(str)
            for code, code_rows in selected.groupby(
                selected["ts_code"].astype(str).str.upper(), sort=False
            ):
                first = str(code_rows["trade_date"].min())
                last = str(code_rows["trade_date"].max())
                remove |= active_codes.eq(str(code)) & active_dates.between(first, last)
            active = active[~remove]
        merged = _deduplicate(
            pd.concat([active, selected], ignore_index=True, sort=False), INDEX_DAILY
        )
        write_parquet_atomic(merged, path, INDEX_DAILY)
        changed.add(relative.as_posix())


def _merge_calendar(
    staged: Path, update: pd.DataFrame, end_session: str
) -> pd.DataFrame:
    relative = parquet_relative_path(TRADE_CAL, exchange="SSE")
    active = _read_existing(staged / relative, TRADE_CAL)
    combined = pd.concat([active, update], ignore_index=True, sort=False)
    combined = normalize_frame(combined, TRADE_CAL)
    combined = combined.drop_duplicates(["exchange", "cal_date"], keep="last")
    combined = combined[combined["cal_date"].astype(str) <= str(end_session)].copy()
    combined = combined.sort_values("cal_date", kind="stable").reset_index(drop=True)
    write_parquet_atomic(combined, staged / relative, TRADE_CAL)
    return combined


def build_parquet_bundle(
    cache_path: str | Path,
    *,
    stock: pd.DataFrame,
    etf: pd.DataFrame,
    index: pd.DataFrame,
    trade_cal: pd.DataFrame,
    bundle_root: str | Path,
    bundle_name: str,
    staging_root: str | Path,
    merge_active: bool = True,
    force_compact: bool = False,
    index_daily_codes: Sequence[str] = INDEX_DAILY_CODES,
    requested_index_daily: pd.DataFrame | None = None,
) -> ParquetBuild:
    del force_compact
    root = Path(bundle_root).expanduser()
    cache = Path(cache_path)
    staging = Path(staging_root)
    staged = staging / "parquet-bundle"
    shutil.rmtree(staged, ignore_errors=True)
    active = find_active_bundle(root, bundle_name) if merge_active else None
    previous_manifest: dict[str, object] | None = None
    if active is not None:
        shutil.copytree(active, staged, copy_function=_hardlink_or_copy)
        previous_manifest = load_manifest(active)
        (staged / MANIFEST_FILE).unlink(missing_ok=True)
        (staged / CATALOG_FILE).unlink(missing_ok=True)
    else:
        (staged / PARQUET_DIRECTORY).mkdir(parents=True, exist_ok=False)

    allowed_index_daily_codes = tuple(
        sorted(
            {
                *(str(code).upper() for code in index_daily_codes),
                *_stored_index_daily_codes(staged),
            }
        )
    )
    if not allowed_index_daily_codes:
        raise DataError("index_daily code allowlist cannot be empty")
    changed: set[str] = {CATALOG_FILE}
    _prune_index_daily(staged, changed, allowed_index_daily_codes)
    write_parquet_atomic(
        stock, staged / parquet_relative_path(STOCK_BASIC), STOCK_BASIC
    )
    write_parquet_atomic(etf, staged / parquet_relative_path(ETF_BASIC), ETF_BASIC)
    write_parquet_atomic(
        index, staged / parquet_relative_path(INDEX_BASIC), INDEX_BASIC
    )
    changed.update(
        {
            parquet_relative_path(STOCK_BASIC).as_posix(),
            parquet_relative_path(ETF_BASIC).as_posix(),
            parquet_relative_path(INDEX_BASIC).as_posix(),
        }
    )
    metadata = json.loads((cache / "cache-manifest.json").read_text(encoding="utf-8"))
    target_dates = {str(value) for value in metadata.get("target_dates", [])}
    raw_targets = metadata.get("target_dates_by_dataset", {})
    targets_by_dataset = (
        {
            str(dataset): {str(value) for value in values}
            for dataset, values in raw_targets.items()
        }
        if isinstance(raw_targets, dict) and raw_targets
        else {}
    )
    safe_end = str(metadata.get("safe_end") or "")
    if not safe_end:
        if not target_dates:
            raise DataError("update cache contains neither target dates nor safe end")
        safe_end = max(target_dates)
    calendar = _merge_calendar(staged, trade_cal, safe_end)
    changed.add(parquet_relative_path(TRADE_CAL, exchange="SSE").as_posix())

    etf_codes = set(etf["ts_code"].astype(str).str.upper())
    daily_specs = (
        STOCK_DAILY,
        ETF_DAILY,
        INDEX_DAILY,
        ADJ_FACTOR,
        ETF_ADJ_FACTOR,
        DAILY_BASIC,
        MONEYFLOW,
        STK_LIMIT,
        SUSPEND_D,
        STOCK_ST,
        INDUSTRY,
    )
    for spec in daily_specs:
        spec_target_dates = targets_by_dataset.get(
            DAILY_DATASET_BY_TABLE[spec.name], target_dates
        )
        if not spec_target_dates:
            continue
        incoming = _daily_incoming(
            cache, spec, etf_codes, set(allowed_index_daily_codes)
        )
        for year in sorted({date[:4] for date in spec_target_dates}):
            _replace_daily_year(
                staged, spec, year, spec_target_dates, incoming, changed
            )
    _merge_requested_index_daily(
        staged,
        requested_index_daily if requested_index_daily is not None else pd.DataFrame(),
        changed,
    )
    _merge_finance(staged, cache, changed)
    _merge_index_weights(staged, cache, changed)
    _validate_critical_data(staged, allowed_index_daily_codes)

    observations = daily_observations(staged)
    open_calendar = calendar[calendar["is_open"].astype(str).isin({"1", "1.0"})]
    open_dates = sorted(open_calendar["cal_date"].astype(str).unique())
    if not open_dates:
        raise DataError("updated calendar contains no open SSE dates")
    assets = build_asset_table(
        bundle_path(root, bundle_name),
        stock,
        etf,
        index,
        observations,
        open_dates[0],
        open_dates[-1],
    )
    generation = str(uuid4())
    datasets = datasets_frame(staged)
    build_catalog(
        staged,
        generation=generation,
        assets=assets,
        trade_calendar=calendar,
        datasets=datasets,
    )
    previous_files = (
        {
            relative: entry
            for relative, entry in dict(previous_manifest.get("files", {})).items()
            if relative not in changed
        }
        if previous_manifest is not None
        else None
    )
    manifest = create_manifest(
        staged,
        generation=generation,
        start_session=open_dates[0],
        end_session=open_dates[-1],
        session_count=len(open_dates),
        asset_count=int(assets["tradable"].sum()),
        index_count=int((assets["asset_type"] == "index").sum()),
        build_pipeline="tushare->partitioned-csv->year-partitioned-parquet",
        previous_files=previous_files,
    )
    validate_bundle(staged, full_hash=False)
    destination = bundle_path(root, bundle_name)
    lock = FileLock(str(bundle_lock_path(root, bundle_name)), thread_local=False)
    with lock:
        publish_fixed_bundle(staged, destination)
    result = BundleBuildResult(
        path=destination,
        start_session=pd.Timestamp(open_dates[0]),
        end_session=pd.Timestamp(open_dates[-1]),
        asset_count=int(assets["tradable"].sum()),
        session_count=len(open_dates),
    )
    with suppress(OSError):
        (staging / "EXPECTED_GENERATION").write_text(generation, encoding="utf-8")
    return ParquetBuild(result, manifest)
