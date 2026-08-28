"""DuckDB/Parquet point-in-time data portal."""

from __future__ import annotations

import os
from bisect import bisect_left
from collections import OrderedDict
from collections.abc import Sequence
from functools import lru_cache
from pathlib import Path
from typing import Any, Self

import numpy as np
import pandas as pd

from ..foundation.config import AdjustmentMode, normalize_session
from ..foundation.exceptions import DataError
from ..model.asset import Asset, AssetFinder
from .bundle.manager import (
    BUNDLE_NAME,
    acquire_bundle_read_lock,
    latest_bundle_path,
    release_bundle_read_lock,
)
from .bundle.parquet_schema import FINANCE_SPECS
from .bundle.parquet_store import load_manifest
from .factors import (
    available_operators,
    compile_expression,
    evaluate_expressions,
    is_factor_expression,
)
from .query import LocalDataClient
from .trading_calendar import ChinaTradingCalendar

_ADJUSTED_PRICE_FIELDS = frozenset({"open", "high", "low", "close", "price"})
_BASE_FIELDS = frozenset(
    {
        "open",
        "high",
        "low",
        "close",
        "price",
        "pre_close",
        "volume",
        "turnover",
        "up_limit",
        "down_limit",
        "adj_factor",
        "suspended",
    }
)
_EXTENDED_DAILY_NAMESPACES = frozenset(
    {"daily_basic", "moneyflow", "industry", "stock_st"}
)
_FINANCIAL_NAMESPACES = frozenset(FINANCE_SPECS)
_FINANCIAL_ROW_COLUMNS = (
    "effective_ann_date",
    "end_date",
    "report_type",
    "update_flag",
    "source_order",
)
_DEFAULT_COLUMN_CACHE_MIB = 2048
_INDEX_FRAME_CACHE_SIZE = 128
_DIRECT_WINDOW_MIN_ASSETS = 64
_INDEX_DAILY_FIELDS = frozenset(
    {"open", "high", "low", "close", "price", "pre_close", "volume", "turnover"}
)

_BASE_STORAGE = {
    "open": ("daily", "open"),
    "high": ("daily", "high"),
    "low": ("daily", "low"),
    "close": ("daily", "close"),
    "price": ("daily", "close"),
    "pre_close": ("daily", "pre_close"),
    "volume": ("daily", "volume"),
    "turnover": ("daily", "turnover"),
    "up_limit": ("stk_limit", "up_limit"),
    "down_limit": ("stk_limit", "down_limit"),
    "adj_factor": ("adj_factor", "adj_factor"),
    "suspended": ("suspend_d", "suspended"),
}

_DAILY_TABLES = {
    "daily": ("stock_daily", "etf_daily"),
    "adj_factor": ("adj_factor", "etf_adj_factor"),
    "daily_basic": ("daily_basic",),
    "moneyflow": ("moneyflow",),
    "stk_limit": ("stk_limit",),
    "suspend_d": ("suspend_d",),
    "stock_st": ("stock_st",),
    "industry": ("industry",),
}

_DAILY_FIELDS: dict[str, dict[str, Any]] = {
    **{
        f"daily_basic.{name}": {
            "role": "daily_basic",
            "column": name,
            "kind": "numeric",
        }
        for name in [
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
        ]
    },
    **{
        f"moneyflow.{name}": {"role": "moneyflow", "column": name, "kind": "numeric"}
        for name in [
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
        ]
    },
    **{
        f"industry.{name}": {"role": "industry", "column": name, "kind": "categorical"}
        for name in ["l1_code", "l1_name", "l2_code", "l2_name", "l3_code", "l3_name"]
    },
    **{
        f"stock_st.{name}": {"role": "stock_st", "column": name, "kind": "categorical"}
        for name in ["name", "type", "type_name"]
    },
    "stock_st.is_st": {"role": "stock_st", "column": "is_st", "kind": "flag"},
}


def _default_column_cache_bytes() -> int:
    raw = os.environ.get("TUALPHA_COLUMN_CACHE_MIB", str(_DEFAULT_COLUMN_CACHE_MIB))
    try:
        mib = int(raw)
    except ValueError as exc:
        raise DataError("TUALPHA_COLUMN_CACHE_MIB must be an integer") from exc
    if mib < 0:
        raise DataError("TUALPHA_COLUMN_CACHE_MIB must not be negative")
    return mib * 1024**2


def _as_float(value: object, default: float = np.nan) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if np.isfinite(result) else default


class BundleDataPortal:
    """Read daily bars and PIT datasets from year-partitioned Parquet."""

    def __init__(
        self,
        bundle_root: str | Path,
        asset_finder: AssetFinder,
        calendar: ChinaTradingCalendar,
        adjustment: AdjustmentMode | str,
        backtest_end: str | pd.Timestamp,
        bundle_name: str = BUNDLE_NAME,
        *,
        column_cache_max_bytes: int | None = None,
    ) -> None:
        from .bar import DailyBar

        self._daily_bar_type = DailyBar
        self.bundle_root = Path(bundle_root).expanduser()
        self.asset_finder = asset_finder
        self.calendar = calendar
        self.adjustment = AdjustmentMode(adjustment)
        self.backtest_end = normalize_session(backtest_end)
        self._column_cache_max_bytes = (
            _default_column_cache_bytes()
            if column_cache_max_bytes is None
            else int(column_cache_max_bytes)
        )
        if self._column_cache_max_bytes < 0:
            raise ValueError("column_cache_max_bytes must not be negative")
        self._bundle_lock_key, _ = acquire_bundle_read_lock(
            self.bundle_root, bundle_name
        )
        self._client: LocalDataClient | None = None
        try:
            self.bundle_path = latest_bundle_path(self.bundle_root, bundle_name)
            self._manifest = load_manifest(self.bundle_path)
            generation = str(self._manifest["generation"])
            if (
                asset_finder.bundle_generation != generation
                or calendar.bundle_generation != generation
            ):
                raise DataError(
                    "bundle components come from different generations; recreate AssetFinder and ChinaTradingCalendar"
                )
            self._client = LocalDataClient(self.bundle_root, bundle_name)
            self._assets_by_sid = {asset.sid: asset for asset in asset_finder}
            self._ordered_assets = tuple(
                sorted(asset_finder, key=lambda item: item.sid)
            )
            self._asset_position = {
                asset.sid: index for index, asset in enumerate(self._ordered_assets)
            }
            self._code_position = {
                asset.ts_code: index for index, asset in enumerate(self._ordered_assets)
            }
            self._sessions = calendar.sessions[calendar.sessions <= self.backtest_end]
            if self._sessions.empty:
                raise DataError("bundle has no sessions on or before the backtest end")
            self._session_values = self._sessions.asi8
            self._session_date_ints = np.asarray(
                self._sessions.year * 10_000
                + self._sessions.month * 100
                + self._sessions.day,
                dtype=np.int32,
            )
            self._list_date_ints = np.asarray(
                [
                    (
                        (
                            asset.list_date.year * 10_000
                            + asset.list_date.month * 100
                            + asset.list_date.day
                        )
                        if asset.list_date is not None
                        else np.iinfo(np.int32).min
                    )
                    for asset in self._ordered_assets
                ],
                dtype=np.int32,
            )
            self._delist_date_ints = np.asarray(
                [
                    (
                        (
                            asset.delist_date.year * 10_000
                            + asset.delist_date.month * 100
                            + asset.delist_date.day
                        )
                        if asset.delist_date is not None
                        else np.iinfo(np.int32).max
                    )
                    for asset in self._ordered_assets
                ],
                dtype=np.int32,
            )
            self._column_cache: OrderedDict[str, np.ndarray] = OrderedDict()
            self._column_cache_bytes = 0
            self._loaded_positions: dict[str, np.ndarray] = {}
            self._categories: dict[str, tuple[str, ...]] = {}
            self._finance_cache: dict[tuple[int, str], pd.DataFrame] = {}
            self._index_daily_frames: dict[str, pd.DataFrame] = {}
            self._index_constituent_dates: dict[str, tuple[str, ...]] = {}
            self._index_frames: OrderedDict[tuple[str, str], pd.DataFrame] = (
                OrderedDict()
            )
            self._position_prefetch_disabled = False
            asset_positions = pd.DataFrame(
                {
                    "ts_code": tuple(self._code_position),
                    "asset_position": tuple(self._code_position.values()),
                }
            )
            session_positions = pd.DataFrame(
                {
                    "trade_date": self._sessions.strftime("%Y%m%d"),
                    "session_position": np.arange(len(self._sessions), dtype=np.int32),
                }
            )
            self._client.connection.register(
                "_tualpha_asset_position_frame", asset_positions
            )
            self._client.connection.register(
                "_tualpha_session_position_frame", session_positions
            )
            try:
                self._client.connection.execute(
                    "CREATE TEMP TABLE _tualpha_asset_positions AS "
                    "SELECT * FROM _tualpha_asset_position_frame"
                )
                self._client.connection.execute(
                    "CREATE TEMP TABLE _tualpha_session_positions AS "
                    "SELECT * FROM _tualpha_session_position_frame"
                )
            finally:
                self._client.connection.unregister("_tualpha_asset_position_frame")
                self._client.connection.unregister("_tualpha_session_position_frame")
            self._bar_presence: np.ndarray | None = None
            self._financial_fields = {
                f"{table}.{column}": (table, column)
                for table, spec in FINANCE_SPECS.items()
                for column in spec.column_names
                if column
                not in {
                    "ts_code",
                    "ann_date",
                    "f_ann_date",
                    "effective_ann_date",
                    "end_date",
                    "report_type",
                    "comp_type",
                    "end_type",
                    "update_flag",
                    "source_order",
                }
            }
            self._index_constituent_codes = tuple(
                sorted(
                    str(value[0]).upper()
                    for value in self._client.connection.execute(
                        "SELECT DISTINCT index_code FROM index_weight"
                    ).fetchall()
                )
            )
        except Exception:
            if self._client is not None:
                self._client.close()
            release_bundle_read_lock(self._bundle_lock_key)
            self._bundle_lock_key = None
            raise

    @property
    def _connection(self):
        if self._client is None:
            raise DataError("data portal is closed")
        return self._client.connection

    def close(self) -> None:
        self.clear_cache()
        if self._client is not None:
            self._client.close()
            self._client = None
        if getattr(self, "_bundle_lock_key", None) is not None:
            release_bundle_read_lock(self._bundle_lock_key)
            self._bundle_lock_key = None

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()

    def _extended_daily_spec(self, field: str) -> dict[str, Any] | None:
        if "." not in field:
            return None
        namespace, _ = field.split(".", 1)
        if namespace in _FINANCIAL_NAMESPACES:
            raise KeyError(
                f"financial field {field!r} must be read with fundamental(s)"
            )
        if namespace not in _EXTENDED_DAILY_NAMESPACES:
            raise KeyError(f"unknown data namespace: {namespace!r}")
        try:
            return _DAILY_FIELDS[field]
        except KeyError as exc:
            raise KeyError(f"unknown daily field: {field!r}") from exc

    def _financial_spec(self, field: str) -> tuple[str, str]:
        try:
            return self._financial_fields[field]
        except KeyError as exc:
            raise KeyError(f"unknown financial value field: {field!r}") from exc

    def available_fields(self, namespace: str | None = None) -> tuple[str, ...]:
        if namespace is None:
            return tuple(sorted((*_DAILY_FIELDS, *self._financial_fields)))
        if namespace == "index":
            return tuple(sorted(_INDEX_DAILY_FIELDS))
        if namespace in _EXTENDED_DAILY_NAMESPACES:
            return tuple(
                sorted(
                    field
                    for field in _DAILY_FIELDS
                    if field.startswith(f"{namespace}.")
                )
            )
        if namespace in _FINANCIAL_NAMESPACES:
            return tuple(
                sorted(
                    field
                    for field in self._financial_fields
                    if field.startswith(f"{namespace}.")
                )
            )
        raise KeyError(f"unknown data namespace: {namespace!r}")

    @lru_cache(maxsize=8_192)  # noqa: B019
    def _session_location(self, value: int) -> int | None:
        date = pd.to_datetime(str(value), format="%Y%m%d")
        position = int(self._sessions.searchsorted(date))
        return (
            position
            if position < len(self._sessions) and self._sessions[position] == date
            else None
        )

    def _asset_positions(self, assets: Sequence[Asset]) -> np.ndarray:
        return np.asarray(
            [self._asset_position.get(asset.sid, -1) for asset in assets],
            dtype=np.int64,
        )

    def _alive(self, positions: np.ndarray, sessions: pd.DatetimeIndex) -> np.ndarray:
        valid_positions = positions >= 0
        result = np.zeros((len(sessions), len(positions)), dtype=bool)
        if valid_positions.any():
            # Pandas 3 preserves/infer datetime resolutions instead of always using
            # nanoseconds. Compare date ordinals rather than raw ``asi8`` values.
            values = np.asarray(
                sessions.year * 10_000 + sessions.month * 100 + sessions.day,
                dtype=np.int32,
            )[:, None]
            selected = positions[valid_positions]
            result[:, valid_positions] = (
                self._list_date_ints[selected][None, :] <= values
            ) & (values <= self._delist_date_ints[selected][None, :])
        return result

    def _cache(self, key: str, values: np.ndarray) -> np.ndarray:
        values.setflags(write=False)
        if values.nbytes <= self._column_cache_max_bytes:
            while (
                self._column_cache
                and self._column_cache_bytes + values.nbytes
                > self._column_cache_max_bytes
            ):
                evicted_key, evicted = self._column_cache.popitem(last=False)
                self._column_cache_bytes -= evicted.nbytes
                self._loaded_positions.pop(evicted_key, None)
            self._column_cache[key] = values
            self._column_cache_bytes += values.nbytes
        return values

    def _dense_column_nbytes(self, role: str, field: str) -> int:
        flag = (role, field) in {
            ("stock_st", "is_st"),
            ("suspend_d", "suspended"),
        }
        dtype = np.uint8 if flag else np.float64 if role == "daily" else np.float32
        itemsize = np.dtype(dtype).itemsize
        return len(self._sessions) * len(self._ordered_assets) * itemsize

    def _field_query(self, role: str, field: str) -> tuple[str, list[object]]:
        tables = _DAILY_TABLES[role]
        if role == "suspend_d" and field == "suspended":
            selects = [
                "SELECT trade_date, ts_code, 1::DOUBLE AS value FROM suspend_d WHERE upper(coalesce(suspend_type, '')) = 'S'"
            ]
        elif role == "stock_st" and field == "is_st":
            selects = ["SELECT trade_date, ts_code, 1::DOUBLE AS value FROM stock_st"]
        else:
            selects = [
                f'SELECT trade_date, ts_code, "{field}" AS value FROM "{table}"'
                for table in tables
            ]
        return " UNION ALL ".join(selects), []

    def _daily_field_details(self, field: str) -> tuple[str, str, str]:
        if "." in field:
            spec = self._extended_daily_spec(field)
            assert spec is not None
            return spec["role"], spec["column"], spec["kind"]
        if field not in _BASE_FIELDS:
            raise KeyError(f"unknown daily bar field: {field}")
        role, column = _BASE_STORAGE[field]
        kind = "flag" if field == "suspended" else "numeric"
        return role, column, kind

    def _cache_has_fields(self, positions: np.ndarray, fields: Sequence[str]) -> bool:
        requested = np.unique(positions[positions >= 0])
        for field in fields:
            role, column, _ = self._daily_field_details(field)
            loaded = self._loaded_positions.get(f"{role}.{column}")
            if loaded is None or not bool(loaded[requested].all()):
                return False
        return True

    def _direct_daily_matrices(
        self,
        assets: Sequence[Asset],
        sessions: pd.DatetimeIndex,
        fields: Sequence[str],
    ) -> dict[str, np.ndarray]:
        """Read only the requested window without materializing full columns."""

        asset_list = list(assets)
        names = list(dict.fromkeys(fields))
        positions = self._asset_positions(asset_list)
        alive = self._alive(positions, sessions)
        grouped: dict[str, dict[str, list[str]]] = {}
        kinds: dict[str, str] = {}
        for field in names:
            role, column, kind = self._daily_field_details(field)
            grouped.setdefault(role, {}).setdefault(column, []).append(field)
            kinds[field] = kind

        matrices: dict[str, np.ndarray] = {}
        for field in names:
            if kinds[field] == "categorical":
                matrices[field] = np.full(alive.shape, None, dtype=object)
            elif kinds[field] == "flag":
                matrix = np.zeros(alive.shape, dtype=float)
                matrix[~alive] = np.nan
                matrices[field] = matrix
            else:
                matrices[field] = np.full(alive.shape, np.nan, dtype=float)

        valid_positions = np.unique(positions[positions >= 0])
        if not len(sessions) or not len(valid_positions):
            return matrices
        session_positions = self._sessions.get_indexer(sessions)
        if bool((session_positions < 0).any()):
            return matrices

        global_to_local = np.full(len(self._ordered_assets), -1, dtype=np.int64)
        duplicate_columns: list[tuple[int, int]] = []
        for local, position in enumerate(positions):
            if position < 0:
                continue
            first = global_to_local[position]
            if first >= 0:
                duplicate_columns.append((local, int(first)))
            else:
                global_to_local[position] = local

        start_date = sessions[0].strftime("%Y%m%d")
        end_date = sessions[-1].strftime("%Y%m%d")
        for role, physical_fields in grouped.items():
            selections = []
            for column in physical_fields:
                if role == "suspend_d" and column == "suspended":
                    selections.append("1::DOUBLE AS suspended")
                elif role == "stock_st" and column == "is_st":
                    selections.append("1::DOUBLE AS is_st")
                else:
                    selections.append(f'"{column}"')
            where = (
                " WHERE upper(coalesce(suspend_type, '')) = 'S'"
                if role == "suspend_d"
                else ""
            )
            selects = [
                f"SELECT trade_date, ts_code, {', '.join(selections)} "
                f'FROM "{table}"{where}'
                for table in _DAILY_TABLES[role]
            ]
            query = (
                "SELECT sessions.session_position, assets.asset_position, "
                + ", ".join(f'source_rows."{column}"' for column in physical_fields)
                + " FROM ("
                + " UNION ALL ".join(selects)
                + ") source_rows "
                "JOIN _tualpha_asset_positions assets "
                "ON upper(source_rows.ts_code) = assets.ts_code "
                "JOIN _tualpha_session_positions sessions "
                "ON source_rows.trade_date = sessions.trade_date "
                "JOIN unnest(?::BIGINT[]) requested(asset_position) "
                "ON assets.asset_position = requested.asset_position "
                "WHERE source_rows.trade_date >= ? AND source_rows.trade_date <= ?"
            )
            reader = self._connection.execute(
                query,
                [valid_positions.tolist(), start_date, end_date],
            ).to_arrow_reader(batch_size=500_000)
            for batch in reader:
                global_rows = np.asarray(
                    batch.column("session_position").to_numpy(zero_copy_only=False),
                    dtype=np.int64,
                )
                rows = np.searchsorted(session_positions, global_rows)
                global_columns = np.asarray(
                    batch.column("asset_position").to_numpy(zero_copy_only=False),
                    dtype=np.int64,
                )
                columns = global_to_local[global_columns]
                valid = (
                    (rows >= 0)
                    & (rows < len(sessions))
                    & (columns >= 0)
                    & alive[rows, columns]
                )
                for physical, logical_fields in physical_fields.items():
                    values = batch.column(physical)
                    for field in logical_fields:
                        if kinds[field] == "categorical":
                            decoded = np.asarray(values.to_pylist(), dtype=object)
                        else:
                            decoded = np.asarray(
                                values.to_numpy(zero_copy_only=False), dtype=float
                            )
                        matrices[field][rows[valid], columns[valid]] = decoded[valid]

        for duplicate, first in duplicate_columns:
            for matrix in matrices.values():
                matrix[:, duplicate] = matrix[:, first]
        for field in {"volume", "turnover"}.intersection(matrices):
            matrices[field] = np.maximum(matrices[field], 0.0)
        return matrices

    def _direct_factor_matrix(
        self, assets: Sequence[Asset], sessions: pd.DatetimeIndex
    ) -> np.ndarray:
        factors = self._direct_daily_matrices(assets, sessions, ["adj_factor"])[
            "adj_factor"
        ]
        positions = self._asset_positions(assets)
        valid_positions = np.unique(positions[positions >= 0])
        previous = np.ones(len(assets), dtype=float)
        if len(valid_positions) and len(sessions):
            source, params = self._field_query("adj_factor", "adj_factor")
            query = (
                "SELECT assets.asset_position, "
                "arg_max(source_rows.value, source_rows.trade_date) AS value "
                f"FROM ({source}) source_rows "
                "JOIN _tualpha_asset_positions assets "
                "ON upper(source_rows.ts_code) = assets.ts_code "
                "JOIN unnest(?::BIGINT[]) requested(asset_position) "
                "ON assets.asset_position = requested.asset_position "
                "WHERE source_rows.trade_date < ? AND source_rows.value > 0 "
                "GROUP BY assets.asset_position"
            )
            rows = self._connection.execute(
                query,
                [*params, valid_positions.tolist(), sessions[0].strftime("%Y%m%d")],
            ).fetchall()
            by_position = {int(position): float(value) for position, value in rows}
            previous = np.asarray(
                [by_position.get(int(position), 1.0) for position in positions],
                dtype=float,
            )
        for row in range(len(sessions)):
            valid = np.isfinite(factors[row]) & (factors[row] > 0)
            previous[valid] = factors[row, valid]
            factors[row, ~valid] = previous[~valid]
        return factors

    def _direct_adjustment_matrix(
        self,
        assets: Sequence[Asset],
        sessions: pd.DatetimeIndex,
        reference_session: str | pd.Timestamp,
    ) -> np.ndarray:
        if (
            self.adjustment is AdjustmentMode.RAW
            or not len(sessions)
            or not len(assets)
        ):
            return np.ones((len(sessions), len(assets)), dtype=float)
        factors = self._direct_factor_matrix(assets, sessions)
        if self.adjustment is AdjustmentMode.HFQ:
            return factors
        reference_date = normalize_session(reference_session)
        if normalize_session(sessions[-1]) == reference_date:
            # history() rebases qfq prices to its last row. Reuse the factors
            # already read for the window instead of scanning Parquet again.
            references = factors[-1].copy()
        else:
            references = self._direct_factor_matrix(
                assets, pd.DatetimeIndex([reference_date])
            )[0]
        references[~np.isfinite(references) | (references <= 0)] = 1.0
        return factors / references[None, :]

    @staticmethod
    def _history_frame(
        assets: Sequence[Asset],
        fields: Sequence[str],
        sessions: pd.DatetimeIndex,
        matrices: dict[str, np.ndarray],
    ) -> pd.DataFrame:
        codes = [asset.ts_code for asset in assets]
        names = list(fields)
        columns = pd.MultiIndex.from_product([codes, names], names=["asset", "field"])
        has_strings = any(matrices[field].dtype == object for field in names)
        if not has_strings:
            values = np.stack([matrices[field] for field in names], axis=2).reshape(
                len(sessions), len(codes) * len(names)
            )
            return pd.DataFrame(values, index=sessions, columns=columns)
        pieces = {
            field: pd.DataFrame(matrices[field], index=sessions, columns=codes)
            for field in names
        }
        result = pd.concat(pieces, axis=1).swaplevel(0, 1, axis=1)
        result.columns.names = ["asset", "field"]
        return result.reindex(columns=columns)

    def _full_column(
        self,
        role: str,
        field: str,
        positions: np.ndarray | None = None,
    ) -> np.ndarray:
        key = f"{role}.{field}"
        cached = self._column_cache.get(key)
        categorical = role in {"industry", "stock_st"} and field != "is_st"
        if cached is None:
            if categorical:
                cached = np.full(
                    (len(self._sessions), len(self._ordered_assets)),
                    -1,
                    dtype=np.int32,
                )
            else:
                flag = (role, field) in {
                    ("stock_st", "is_st"),
                    ("suspend_d", "suspended"),
                }
                dtype = (
                    np.uint8 if flag else np.float64 if role == "daily" else np.float32
                )
                cached = np.full(
                    (len(self._sessions), len(self._ordered_assets)),
                    0 if flag else np.nan,
                    dtype=dtype,
                )
            self._loaded_positions[key] = np.zeros(
                len(self._ordered_assets), dtype=bool
            )
            cached = self._cache(key, cached)
        else:
            self._column_cache.move_to_end(key)

        loaded = self._loaded_positions.setdefault(
            key, np.zeros(len(self._ordered_assets), dtype=bool)
        )
        raw_requested = (
            np.arange(len(self._ordered_assets), dtype=np.int64)
            if positions is None
            else np.asarray(positions, dtype=np.int64)
        )
        in_range = (raw_requested >= 0) & (raw_requested < len(self._ordered_assets))
        if bool(in_range.all()) and bool(loaded[raw_requested].all()):
            return cached
        requested = np.unique(raw_requested[in_range])
        missing = requested[~loaded[requested]]
        if not len(missing):
            return cached

        query, params = self._field_query(role, field)
        placeholders = ", ".join("?" for _ in missing)
        query = (
            "SELECT sessions.session_position, assets.asset_position, "
            "source_rows.value FROM ("
            f"{query}) source_rows "
            "JOIN _tualpha_asset_positions assets "
            "ON upper(source_rows.ts_code) = assets.ts_code "
            "JOIN _tualpha_session_positions sessions "
            "ON source_rows.trade_date = sessions.trade_date "
            f"WHERE assets.asset_position IN ({placeholders})"
        )
        reader = self._connection.execute(
            query, [*params, *[int(value) for value in missing]]
        ).to_arrow_reader(batch_size=500_000)
        cached.setflags(write=True)
        for batch in reader:
            date_positions = np.asarray(
                batch.column("session_position").to_numpy(zero_copy_only=False),
                dtype=np.int64,
            )
            asset_positions = np.asarray(
                batch.column("asset_position").to_numpy(zero_copy_only=False),
                dtype=np.int64,
            )
            valid = (date_positions >= 0) & (asset_positions >= 0)
            values = batch.column("value")
            if categorical:
                text = [
                    "" if value is None else str(value) for value in values.to_pylist()
                ]
                categories = list(self._categories.get(key, ()))
                known = set(categories)
                categories.extend(
                    value
                    for value in dict.fromkeys(text)
                    if value and value not in known
                )
                self._categories[key] = tuple(categories)
                mapping = {value: index for index, value in enumerate(categories)}
                encoded = np.asarray(
                    [mapping.get(value, -1) for value in text], dtype=np.int32
                )
                cached[date_positions[valid], asset_positions[valid]] = encoded[valid]
            else:
                decoded = np.asarray(values.to_numpy(zero_copy_only=False), dtype=float)
                cached[date_positions[valid], asset_positions[valid]] = decoded[valid]
        loaded[missing] = True
        cached.setflags(write=False)
        return cached

    def _bar_presence_array(self) -> np.ndarray:
        if self._bar_presence is None:
            present = np.isfinite(
                self._full_column("daily", "close")
            ) | self._full_column("suspend_d", "suspended").astype(bool)
            present.setflags(write=False)
            self._bar_presence = present
        return self._bar_presence

    @lru_cache(maxsize=16_384)  # noqa: B019
    def _factor_series(self, sid: int) -> tuple[np.ndarray, np.ndarray]:
        position = self._asset_position.get(sid)
        if position is None:
            return np.array([], dtype=np.int32), np.array([], dtype=float)
        values = self._full_column(
            "adj_factor", "adj_factor", np.asarray([position], dtype=np.int64)
        )[:, position]
        valid = np.isfinite(values) & (values > 0)
        return self._session_date_ints[valid], values[valid]

    @lru_cache(maxsize=131_072)  # noqa: B019
    def factor(self, asset: Asset, session: pd.Timestamp) -> float:
        dates, factors = self._factor_series(asset.sid)
        if not len(dates):
            return 1.0
        date = (
            session
            if isinstance(session, pd.Timestamp)
            and session.tzinfo is None
            and session.hour == 0
            and session.minute == 0
            and session.second == 0
            and session.microsecond == 0
            and session.nanosecond == 0
            else normalize_session(session)
        )
        value = date.year * 10_000 + date.month * 100 + date.day
        index = int(np.searchsorted(dates, value, side="right") - 1)
        return float(factors[index]) if index >= 0 else 1.0

    def adjustment_multiplier(
        self,
        asset: Asset,
        session: str | pd.Timestamp,
        *,
        reference_session: str | pd.Timestamp | None = None,
    ) -> float:
        if self.adjustment is AdjustmentMode.RAW:
            return 1.0
        date = normalize_session(session)
        if self.adjustment is AdjustmentMode.QFQ and reference_session is None:
            return 1.0
        factor = self.factor(asset, date)
        if self.adjustment is AdjustmentMode.HFQ:
            return factor
        reference_date = (
            date if reference_session is None else normalize_session(reference_session)
        )
        if reference_date < date:
            raise DataError("qfq reference session cannot precede the price session")
        reference = self.factor(asset, reference_date)
        return factor / reference if reference else 1.0

    def _matrix_locations(
        self, assets: Sequence[Asset], sessions: pd.DatetimeIndex
    ) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        asset_positions = self._asset_positions(assets)
        session_positions = self._sessions.get_indexer(sessions)
        valid_sessions = session_positions >= 0
        alive = self._alive(asset_positions, sessions)
        valid = alive & valid_sessions[:, None] & (asset_positions[None, :] >= 0)
        return session_positions, asset_positions, valid

    def _adjustment_matrix(
        self,
        assets: Sequence[Asset],
        sessions: pd.DatetimeIndex,
        reference_session: str | pd.Timestamp,
    ) -> np.ndarray:
        result = np.ones((len(sessions), len(assets)), dtype=float)
        if (
            self.adjustment is AdjustmentMode.RAW
            or not len(sessions)
            or not len(assets)
        ):
            return result
        session_positions = self._sessions.get_indexer(sessions)
        positions = self._asset_positions(assets)
        valid_columns = np.flatnonzero(positions >= 0)
        if not len(valid_columns) or bool((session_positions < 0).any()):
            return result

        valid_positions = positions[valid_columns]
        factors = self._full_column("adj_factor", "adj_factor", valid_positions)
        selected = factors[np.ix_(session_positions, valid_positions)].astype(
            float, copy=True
        )
        missing_columns = np.flatnonzero(
            (~np.isfinite(selected) | (selected <= 0)).any(axis=0)
        )
        session_dates = self._session_date_ints[session_positions]
        for local_column in missing_columns:
            asset = assets[int(valid_columns[local_column])]
            dates, values = self._factor_series(asset.sid)
            if not len(dates):
                selected[:, local_column] = 1.0
                continue
            indices = np.searchsorted(dates, session_dates, side="right") - 1
            visible = indices >= 0
            selected[:, local_column] = 1.0
            selected[visible, local_column] = values[indices[visible]]

        if self.adjustment is AdjustmentMode.HFQ:
            result[:, valid_columns] = selected
            return result

        reference_date = normalize_session(reference_session)
        reference = (
            reference_date.year * 10_000
            + reference_date.month * 100
            + reference_date.day
        )
        reference_position = int(
            np.searchsorted(self._session_date_ints, reference, side="right") - 1
        )
        references = np.ones(len(valid_positions), dtype=float)
        if reference_position >= 0:
            direct = factors[reference_position, valid_positions].astype(float)
            valid_direct = np.isfinite(direct) & (direct > 0)
            references[valid_direct] = direct[valid_direct]
            for local_column in np.flatnonzero(~valid_direct):
                asset = assets[int(valid_columns[local_column])]
                dates, values = self._factor_series(asset.sid)
                index = int(np.searchsorted(dates, reference, side="right") - 1)
                if index >= 0:
                    references[local_column] = values[index]
        result[:, valid_columns] = selected / references
        return result

    def value(
        self,
        asset: Asset,
        session: str | pd.Timestamp,
        field: str,
        *,
        adjusted: bool = True,
        reference_session: str | pd.Timestamp | None = None,
    ) -> object:
        self._daily_field_details(field)
        date = normalize_session(session)
        if not asset.is_alive_on(date):
            return np.nan
        value = self.values(
            [asset],
            date,
            [field],
            adjusted=adjusted,
            reference_session=reference_session,
        )[field][0]
        return value.item() if isinstance(value, np.generic) else value

    def prefetch(self, assets: Sequence[Asset], fields: Sequence[str]) -> None:
        """Cache full columns for a repeatedly queried fixed asset/field set."""

        requested = list(dict.fromkeys(fields))
        if (
            self.adjustment is not AdjustmentMode.RAW
            and any(field in _ADJUSTED_PRICE_FIELDS for field in requested)
            and "adj_factor" not in requested
        ):
            requested.append("adj_factor")
        positions = self._asset_positions(assets)
        load_positions = positions[positions >= 0]
        for field in requested:
            if "." in field:
                spec = self._extended_daily_spec(field)
                assert spec is not None
                self._full_column(spec["role"], spec["column"], load_positions)
                continue
            if field not in _BASE_FIELDS:
                raise KeyError(f"unknown daily bar field: {field}")
            role, column = _BASE_STORAGE[field]
            self._full_column(role, column, load_positions)

    def prepare_position_data(self, assets: Sequence[Asset]) -> bool:
        """Warm bounded columns reused by daily valuation and position capture."""

        asset_list = list(assets)
        if not asset_list:
            return True
        if self._position_prefetch_disabled:
            return False
        requested = ["close", "pre_close"]
        cache_fields = [*requested]
        if self.adjustment is not AdjustmentMode.RAW:
            cache_fields.append("adj_factor")
        physical = {_BASE_STORAGE[field] for field in cache_fields}
        required_bytes = sum(
            self._dense_column_nbytes(role, column) for role, column in physical
        )
        if required_bytes > self._column_cache_max_bytes:
            return False
        try:
            self.prefetch(asset_list, requested)
        except MemoryError:
            # Keep the run alive on memory-constrained machines. ``values()``
            # will use bounded window queries for this large position basket.
            self.clear_cache()
            self._position_prefetch_disabled = True
            return False
        return self._cache_has_fields(self._asset_positions(asset_list), cache_fields)

    def values(
        self,
        assets: Sequence[Asset],
        session: str | pd.Timestamp,
        fields: Sequence[str],
        *,
        adjusted: bool = True,
        reference_session: str | pd.Timestamp | None = None,
    ) -> dict[str, np.ndarray]:
        date = normalize_session(session)
        session_position = self._sessions.get_indexer([date])[0]
        positions = self._asset_positions(assets)
        alive = self._alive(positions, pd.DatetimeIndex([date]))[0]
        valid = alive & (positions >= 0) & (session_position >= 0)
        load_positions = positions[positions >= 0]
        output: dict[str, np.ndarray] = {}
        adjustments = None
        needs_adjustment = adjusted and (
            self.adjustment is AdjustmentMode.HFQ
            or (self.adjustment is AdjustmentMode.QFQ and reference_session is not None)
        )
        price_fields = any(field in _ADJUSTED_PRICE_FIELDS for field in fields)
        cache_fields = list(fields)
        if needs_adjustment and price_fields:
            cache_fields.append("adj_factor")
        use_window_query = (
            len(assets) >= _DIRECT_WINDOW_MIN_ASSETS
            or self._column_cache_max_bytes == 0
        )
        if use_window_query and not self._cache_has_fields(positions, cache_fields):
            matrices = self._direct_daily_matrices(
                assets, pd.DatetimeIndex([date]), fields
            )
            if needs_adjustment and price_fields:
                reference = date if reference_session is None else reference_session
                direct_adjustments = self._direct_adjustment_matrix(
                    assets, pd.DatetimeIndex([date]), reference
                )[0]
                for field in fields:
                    if field in _ADJUSTED_PRICE_FIELDS:
                        matrices[field][0] *= direct_adjustments
            return {field: matrices[field][0] for field in fields}
        if needs_adjustment and price_fields:
            reference = date if reference_session is None else reference_session
            adjustments = self._adjustment_matrix(
                assets, pd.DatetimeIndex([date]), reference
            )[0]
        for field in fields:
            if "." in field:
                spec = self._extended_daily_spec(field)
                assert spec is not None
                if spec["kind"] == "categorical":
                    result = np.full(len(assets), None, dtype=object)
                elif field == "stock_st.is_st":
                    result = np.zeros(len(assets), dtype=float)
                    result[~alive] = np.nan
                else:
                    result = np.full(len(assets), np.nan, dtype=float)
                if valid.any():
                    raw = self._full_column(
                        spec["role"], spec["column"], load_positions
                    )[session_position, positions[valid]]
                    if spec["kind"] == "categorical":
                        categories = self._categories.get(
                            f"{spec['role']}.{spec['column']}", ()
                        )
                        result[valid] = [
                            (
                                categories[int(value)]
                                if 0 <= int(value) < len(categories)
                                else None
                            )
                            for value in raw
                        ]
                    else:
                        result[valid] = raw.astype(float)
                output[field] = result
                continue
            if field not in _BASE_FIELDS:
                raise KeyError(f"unknown daily bar field: {field}")
            result = np.full(len(assets), np.nan, dtype=float)
            if valid.any():
                role, column = _BASE_STORAGE[field]
                result[valid] = self._full_column(role, column, load_positions)[
                    session_position, positions[valid]
                ].astype(float)
                if field in {"volume", "turnover"}:
                    result[valid] = np.maximum(result[valid], 0.0)
                if adjustments is not None and field in _ADJUSTED_PRICE_FIELDS:
                    result[valid] *= adjustments[valid]
            output[field] = result
        return output

    def _daily_history(
        self,
        assets: Sequence[Asset],
        fields: Sequence[str],
        end_session: str | pd.Timestamp,
        bar_count: int,
        *,
        adjusted: bool = True,
        adjustment_reference_session: str | pd.Timestamp | None = None,
    ) -> pd.DataFrame:
        asset_list = list(assets)
        field_list = list(fields)
        sessions = self.calendar.window(end_session, bar_count, include_end=True)
        adjustment_reference = (
            end_session
            if adjustment_reference_session is None
            else adjustment_reference_session
        )
        asset_positions = self._asset_positions(asset_list)
        needs_adjustment = (
            adjusted
            and self.adjustment is not AdjustmentMode.RAW
            and any(field in _ADJUSTED_PRICE_FIELDS for field in field_list)
        )
        cache_fields = [
            *field_list,
            *(("adj_factor",) if needs_adjustment else ()),
        ]
        use_window_query = (
            len(asset_list) >= _DIRECT_WINDOW_MIN_ASSETS
            or self._column_cache_max_bytes == 0
        )
        if use_window_query and not self._cache_has_fields(
            asset_positions, cache_fields
        ):
            matrices = self._direct_daily_matrices(asset_list, sessions, field_list)
            if needs_adjustment:
                direct_adjustments = self._direct_adjustment_matrix(
                    asset_list,
                    sessions,
                    adjustment_reference,
                )
                for field in field_list:
                    if field in _ADJUSTED_PRICE_FIELDS:
                        matrices[field] *= direct_adjustments
            return self._history_frame(asset_list, field_list, sessions, matrices)

        session_positions, asset_positions, valid = self._matrix_locations(
            asset_list, sessions
        )
        alive = self._alive(asset_positions, sessions)
        adjustments = (
            self._adjustment_matrix(
                asset_list,
                sessions,
                adjustment_reference,
            )
            if needs_adjustment
            else None
        )
        matrices: dict[str, np.ndarray] = {}
        for field in field_list:
            if "." in field:
                spec = self._extended_daily_spec(field)
                assert spec is not None
                if spec["kind"] == "categorical":
                    matrix = np.full(valid.shape, None, dtype=object)
                elif field == "stock_st.is_st":
                    matrix = np.zeros(valid.shape, dtype=float)
                    matrix[~alive] = np.nan
                else:
                    matrix = np.full(valid.shape, np.nan, dtype=float)
                rows, cols = np.where(valid)
                if len(rows):
                    raw = self._full_column(
                        spec["role"], spec["column"], asset_positions[cols]
                    )[session_positions[rows], asset_positions[cols]]
                    if spec["kind"] == "categorical":
                        categories = self._categories.get(
                            f"{spec['role']}.{spec['column']}", ()
                        )
                        matrix[rows, cols] = [
                            (
                                categories[int(value)]
                                if 0 <= int(value) < len(categories)
                                else None
                            )
                            for value in raw
                        ]
                    else:
                        matrix[rows, cols] = raw.astype(float)
                matrices[field] = matrix
                continue
            if field not in _BASE_FIELDS:
                raise KeyError(f"unknown daily bar field: {field}")
            matrix = np.full(valid.shape, np.nan, dtype=float)
            rows, cols = np.where(valid)
            if len(rows):
                role, column = _BASE_STORAGE[field]
                matrix[rows, cols] = self._full_column(
                    role, column, asset_positions[cols]
                )[session_positions[rows], asset_positions[cols]].astype(float)
                if field in {"volume", "turnover"}:
                    matrix[rows, cols] = np.maximum(matrix[rows, cols], 0.0)
                if adjustments is not None and field in _ADJUSTED_PRICE_FIELDS:
                    matrix[rows, cols] *= adjustments[rows, cols]
            matrices[field] = matrix
        return self._history_frame(asset_list, field_list, sessions, matrices)

    def factor_history(
        self,
        assets: Sequence[Asset],
        fields: Sequence[str],
        start_session: str | pd.Timestamp,
        end_session: str | pd.Timestamp,
        *,
        adjusted: bool = True,
        allow_future: bool = False,
    ) -> pd.DataFrame:
        """Return raw fields and factor expressions over an explicit date range.

        Extra source sessions needed by nested rolling operators are loaded before
        ``start_session``.  Future sessions are loaded only when ``allow_future``
        is explicitly enabled by the offline research API; callback history never
        enables it.
        """

        asset_list = list(assets)
        names = list(fields)
        if not names:
            requested_sessions = self.calendar.sessions_in_range(
                start_session, end_session
            )
            columns = pd.MultiIndex.from_arrays([[], []], names=["asset", "field"])
            return pd.DataFrame(index=requested_sessions, columns=columns)
        start = normalize_session(start_session)
        end = normalize_session(end_session)
        if start > end:
            raise ValueError("factor history start must not be after end")
        if end > self.backtest_end:
            raise DataError("factor history query cannot exceed backtest end")
        requested_sessions = self.calendar.sessions_in_range(start, end)
        if requested_sessions.empty:
            columns = pd.MultiIndex.from_product(
                [[asset.ts_code for asset in asset_list], names],
                names=["asset", "field"],
            )
            return pd.DataFrame(index=requested_sessions, columns=columns)

        expressions = [name for name in names if is_factor_expression(name)]
        compiled = [compile_expression(expression) for expression in expressions]
        raw_fields = list(
            dict.fromkeys(
                [
                    *(name for name in names if name not in expressions),
                    *(field for expression in compiled for field in expression.fields),
                ]
            )
        )
        for field in raw_fields:
            _, _, kind = self._daily_field_details(field)
            if (
                field
                in {
                    dependency
                    for expression in compiled
                    for dependency in expression.fields
                }
                and kind == "categorical"
            ):
                raise TypeError(
                    f"categorical field {field!r} cannot be used in a factor expression"
                )

        first = int(self._sessions.searchsorted(requested_sessions[0]))
        last = int(self._sessions.searchsorted(requested_sessions[-1]))
        lookback = max((expression.lookback for expression in compiled), default=0)
        lookahead = (
            max((expression.lookahead for expression in compiled), default=0)
            if allow_future
            else 0
        )
        source_first = max(0, first - lookback)
        source_last = min(len(self._sessions) - 1, last + lookahead)
        source_sessions = self._sessions[source_first : source_last + 1]
        raw = self._daily_history(
            asset_list,
            raw_fields,
            source_sessions[-1],
            len(source_sessions),
            adjusted=adjusted,
            adjustment_reference_session=requested_sessions[-1],
        )
        codes = [asset.ts_code for asset in asset_list]
        inputs = {
            field: raw.xs(field, axis=1, level="field").reindex(columns=codes)
            for field in raw_fields
        }
        calculated = evaluate_expressions(expressions, inputs) if expressions else {}
        matrices: dict[str, np.ndarray] = {}
        for name in names:
            values = calculated.get(name, inputs.get(name))
            if values is None:
                raise KeyError(f"factor history value is unavailable: {name!r}")
            matrices[name] = values.reindex(requested_sessions).to_numpy()
        return self._history_frame(asset_list, names, requested_sessions, matrices)

    def history(
        self,
        assets: Sequence[Asset],
        fields: Sequence[str],
        end_session: str | pd.Timestamp,
        bar_count: int,
        *,
        adjusted: bool = True,
    ) -> pd.DataFrame:
        """Return callback-visible raw fields or factor expressions."""

        field_list = list(fields)
        if not any(is_factor_expression(field) for field in field_list):
            return self._daily_history(
                assets,
                field_list,
                end_session,
                bar_count,
                adjusted=adjusted,
            )
        sessions = self.calendar.window(end_session, bar_count, include_end=True)
        return self.factor_history(
            assets,
            field_list,
            sessions[0],
            sessions[-1],
            adjusted=adjusted,
            allow_future=False,
        )

    @staticmethod
    def available_operators() -> tuple[str, ...]:
        """Return factor operators accepted by :meth:`history`."""

        return available_operators()

    @lru_cache(maxsize=131_072)  # noqa: B019
    def raw_bar(self, asset: Asset, session: str | pd.Timestamp):
        date = normalize_session(session)
        if not asset.is_alive_on(date):
            return None
        fields = self.values(
            [asset],
            date,
            [
                "open",
                "high",
                "low",
                "close",
                "pre_close",
                "volume",
                "turnover",
                "up_limit",
                "down_limit",
                "suspended",
            ],
            adjusted=False,
        )
        suspended_value = _as_float(fields["suspended"][0], 0.0)
        if not any(
            np.isfinite(fields[name][0]) for name in ("open", "high", "low", "close")
        ) and not bool(suspended_value):
            return None
        return self._daily_bar_type(
            asset=asset,
            session=date,
            open=_as_float(fields["open"][0]),
            high=_as_float(fields["high"][0]),
            low=_as_float(fields["low"][0]),
            close=_as_float(fields["close"][0]),
            pre_close=_as_float(fields["pre_close"][0]),
            volume=max(0.0, _as_float(fields["volume"][0], 0.0)),
            turnover=max(0.0, _as_float(fields["turnover"][0], 0.0)),
            up_limit=_as_float(fields["up_limit"][0]),
            down_limit=_as_float(fields["down_limit"][0]),
            suspended=bool(suspended_value),
        )

    def execution_bars(
        self, assets: Sequence[Asset], session: str | pd.Timestamp, execution_field: str
    ) -> dict[Asset, Any]:
        if execution_field not in {"open", "close"}:
            raise ValueError("execution_field must be open or close")
        fields = self.values(
            assets,
            session,
            [execution_field, "volume", "up_limit", "down_limit", "suspended"],
            adjusted=False,
        )
        result: dict[Asset, Any] = {}
        date = normalize_session(session)
        for index, asset in enumerate(assets):
            price = _as_float(fields[execution_field][index])
            suspended = bool(_as_float(fields["suspended"][index], 0.0))
            if not np.isfinite(price) and not suspended:
                result[asset] = None
                continue
            result[asset] = self._daily_bar_type(
                asset=asset,
                session=date,
                open=price if execution_field == "open" else np.nan,
                high=np.nan,
                low=np.nan,
                close=price if execution_field == "close" else np.nan,
                pre_close=np.nan,
                volume=max(0.0, _as_float(fields["volume"][index], 0.0)),
                turnover=0.0,
                up_limit=_as_float(fields["up_limit"][index]),
                down_limit=_as_float(fields["down_limit"][index]),
                suspended=suspended,
            )
        return result

    @lru_cache(maxsize=131_072)  # noqa: B019
    def is_tradable(self, asset: Asset, session: str | pd.Timestamp) -> bool:
        bar = self.raw_bar(asset, session)
        return bar is not None and not bar.suspended and bar.volume > 0

    def _financial_frame(
        self,
        asset: Asset,
        table: str,
        value_columns: Sequence[str],
    ) -> pd.DataFrame:
        """Cache one asset's financial history without reading unused wide columns."""

        key = (asset.sid, table)
        spec = FINANCE_SPECS[table]
        required = set(value_columns)
        required.update(
            column for column in _FINANCIAL_ROW_COLUMNS if column in spec.column_names
        )
        cached = self._finance_cache.get(key)
        if cached is not None and required.issubset(cached.columns):
            return cached
        if cached is not None:
            required.update(cached.columns)
        columns = [column for column in spec.column_names if column in required]
        selected = ", ".join(f'"{column}"' for column in columns)
        frame = self._connection.execute(
            f'SELECT {selected} FROM "{table}" WHERE ts_code = ? '
            "ORDER BY end_date, effective_ann_date, source_order",
            [asset.ts_code],
        ).fetchdf()
        self._finance_cache[key] = frame
        return frame

    def _financial_frame_for_table(
        self,
        asset: Asset,
        session: str | pd.Timestamp,
        table: str,
        fields: Sequence[tuple[str, str]],
        *,
        periods: int,
        end_date: pd.Timestamp | None = None,
        report_type: str | None = "1",
    ) -> pd.DataFrame:
        date = normalize_session(session)
        columns = [field for field, _ in fields]
        if not asset.is_alive_on(date):
            return pd.DataFrame(columns=columns)
        frame = self._financial_frame(asset, table, [column for _, column in fields])
        if frame.empty:
            return pd.DataFrame(columns=columns)
        date_text = date.strftime("%Y%m%d")
        visible = frame[
            (frame["effective_ann_date"].astype(str) < date_text)
            & (frame["end_date"].astype(str) <= date_text)
        ].copy()
        if table != "fina_indicator" and report_type is not None:
            visible = visible[visible["report_type"].astype(str) == str(report_type)]
        if end_date is not None:
            visible = visible[
                visible["end_date"].astype(str) == end_date.strftime("%Y%m%d")
            ]
        if visible.empty:
            return pd.DataFrame(columns=columns)
        visible["update_rank"] = (visible["update_flag"].astype(str) == "1").astype(int)
        visible = (
            visible.sort_values(
                ["end_date", "effective_ann_date", "update_rank", "source_order"],
                ascending=[False, False, False, False],
                kind="stable",
            )
            .drop_duplicates("end_date", keep="first")
            .head(periods)
        )
        result = pd.DataFrame(
            {
                field: pd.to_numeric(visible[column], errors="coerce").to_numpy()
                for field, column in fields
            }
        )
        result.index = pd.to_datetime(visible["end_date"].astype(str), format="%Y%m%d")
        result.index.name = "end_date"
        return result

    def fundamental(
        self,
        asset: Asset,
        session: str | pd.Timestamp,
        field: str,
        *,
        period: str | pd.Timestamp = "latest",
        report_type: str | None = "1",
    ) -> float:
        table, column = self._financial_spec(field)
        end_date = None if period == "latest" else normalize_session(period)
        frame = self._financial_frame_for_table(
            asset,
            session,
            table,
            [(field, column)],
            periods=1,
            end_date=end_date,
            report_type=report_type,
        )
        return np.nan if frame.empty else _as_float(frame.iloc[0][field])

    def fundamentals(
        self,
        asset: Asset,
        session: str | pd.Timestamp,
        fields: str | Sequence[str],
        *,
        periods: int = 4,
        report_type: str | None = "1",
    ) -> pd.DataFrame:
        if periods <= 0:
            raise ValueError("periods must be positive")
        names = [fields] if isinstance(fields, str) else list(fields)
        grouped: dict[str, list[tuple[str, str]]] = {}
        for field in names:
            table, column = self._financial_spec(field)
            grouped.setdefault(table, []).append((field, column))
        frames = [
            self._financial_frame_for_table(
                asset,
                session,
                table,
                table_fields,
                periods=periods,
                report_type=report_type,
            )
            for table, table_fields in grouped.items()
        ]
        nonempty = [frame for frame in frames if not frame.empty]
        if not nonempty:
            return pd.DataFrame(
                index=pd.DatetimeIndex([], name="end_date"), columns=names
            )
        result = pd.concat(nonempty, axis=1).sort_index(ascending=False)
        return (
            result.loc[~result.index.duplicated(keep="first")]
            .head(periods)
            .reindex(columns=names)
        )

    def fundamental_arrays(
        self,
        assets: Sequence[Asset],
        session: str | pd.Timestamp,
        fields: str | Sequence[str],
        *,
        report_type: str | None = "1",
    ) -> dict[str, np.ndarray]:
        """Return latest PIT financial values with one DuckDB query per table."""

        asset_list = list(assets)
        names = list(dict.fromkeys([fields] if isinstance(fields, str) else fields))
        grouped: dict[str, list[tuple[str, str]]] = {}
        for field in names:
            table, column = self._financial_spec(field)
            grouped.setdefault(table, []).append((field, column))
        output = {
            field: np.full(len(asset_list), np.nan, dtype=float) for field in names
        }
        if not asset_list or not names:
            for values in output.values():
                values.setflags(write=False)
            return output

        date = normalize_session(session)
        positions = self._asset_positions(asset_list)
        alive = self._alive(positions, pd.DatetimeIndex([date]))[0]
        valid_positions = np.unique(positions[(positions >= 0) & alive])
        if not len(valid_positions):
            for values in output.values():
                values.setflags(write=False)
            return output

        position_to_columns: dict[int, list[int]] = {}
        for index, position in enumerate(positions):
            if position >= 0 and alive[index]:
                position_to_columns.setdefault(int(position), []).append(index)
        date_text = date.strftime("%Y%m%d")
        for table, table_fields in grouped.items():
            packed_values = ", ".join(
                f'{column} := source_rows."{column}"' for _, column in table_fields
            )
            projected_values = ", ".join(
                f'row_values."{column}" AS "{column}"' for _, column in table_fields
            )
            clauses = [
                "source_rows.effective_ann_date < ?",
                "source_rows.end_date <= ?",
            ]
            params: list[object] = [
                valid_positions.tolist(),
                date_text,
                date_text,
            ]
            if table != "fina_indicator" and report_type is not None:
                clauses.append("source_rows.report_type = ?")
                params.append(str(report_type))
            query = (
                "WITH latest AS ("
                "SELECT assets.asset_position, "
                f"arg_max(struct_pack({packed_values}), "
                "struct_pack("
                "end_date := source_rows.end_date, "
                "effective_ann_date := source_rows.effective_ann_date, "
                "update_rank := CASE WHEN source_rows.update_flag = '1' THEN 1 ELSE 0 END, "
                "source_order := source_rows.source_order"
                ")) AS row_values "
                f'FROM "{table}" source_rows '
                "JOIN _tualpha_asset_positions assets "
                "ON upper(source_rows.ts_code) = assets.ts_code "
                "JOIN unnest(?::BIGINT[]) requested(asset_position) "
                "ON assets.asset_position = requested.asset_position "
                f"WHERE {' AND '.join(clauses)} "
                "GROUP BY assets.asset_position"
                ") SELECT asset_position, "
                f"{projected_values} FROM latest"
            )
            reader = self._connection.execute(query, params).to_arrow_reader(
                batch_size=100_000
            )
            for batch in reader:
                batch_positions = np.asarray(
                    batch.column("asset_position").to_numpy(zero_copy_only=False),
                    dtype=np.int64,
                )
                for field, column in table_fields:
                    values = np.asarray(
                        batch.column(column).to_numpy(zero_copy_only=False),
                        dtype=float,
                    )
                    for row, position in enumerate(batch_positions):
                        for destination in position_to_columns[int(position)]:
                            output[field][destination] = values[row]

        for values in output.values():
            values.setflags(write=False)
        return output

    def _index_daily_frame(
        self, index_code: str, fields: Sequence[str]
    ) -> pd.DataFrame:
        """Incrementally cache only requested index daily columns."""

        code = index_code.upper().strip()
        physical_fields = list(
            dict.fromkeys("close" if field == "price" else field for field in fields)
        )
        cached = self._index_daily_frames.get(code)
        if cached is not None and set(physical_fields).issubset(cached.columns):
            return cached
        selected_fields = list(
            dict.fromkeys(
                [*(cached.columns if cached is not None else ()), *physical_fields]
            )
        )
        selected = ", ".join(
            ("trade_date", *(f'"{column}"' for column in selected_fields))
        )
        frame = self._connection.execute(
            f"SELECT {selected} FROM index_daily "
            "WHERE ts_code = ? AND trade_date <= ? ORDER BY trade_date",
            [code, self.backtest_end.strftime("%Y%m%d")],
        ).fetchdf()
        if frame.empty:
            raise KeyError(f"index daily data are unavailable for {index_code!r}")
        frame.index = pd.to_datetime(
            frame.pop("trade_date").astype(str), format="%Y%m%d"
        )
        frame.index.name = "trade_date"
        for column in selected_fields:
            frame[column] = pd.to_numeric(frame[column], errors="coerce").astype(float)
        for column in {"volume", "turnover"}.intersection(selected_fields):
            frame[column] = frame[column].clip(lower=0.0)
        self._index_daily_frames[code] = frame
        return frame

    @staticmethod
    def _validate_index_daily_fields(fields: Sequence[str]) -> list[str]:
        names = list(fields)
        unknown = set(names).difference(_INDEX_DAILY_FIELDS)
        if unknown:
            raise KeyError(f"unknown index daily field: {min(unknown)}")
        return names

    def index_values(
        self,
        index_code: str,
        session: str | pd.Timestamp,
        fields: Sequence[str],
    ) -> dict[str, float]:
        """Return multiple raw index values with one projected cache load."""

        date = normalize_session(session)
        if date > self.backtest_end:
            raise DataError("index daily query cannot exceed backtest end")
        names = self._validate_index_daily_fields(fields)
        frame = self._index_daily_frame(index_code, names)
        if date not in frame.index:
            return {field: np.nan for field in names}
        return {
            field: _as_float(frame.at[date, "close" if field == "price" else field])
            for field in names
        }

    def index_value(
        self,
        index_code: str,
        session: str | pd.Timestamp,
        field: str,
    ) -> float:
        """Return one raw index daily value visible on ``session``."""

        return self.index_values(index_code, session, [field])[field]

    def index_history(
        self,
        index_code: str,
        fields: Sequence[str],
        end_session: str | pd.Timestamp,
        bar_count: int,
    ) -> pd.DataFrame:
        """Return raw index daily fields over a callback-visible session window."""

        date = normalize_session(end_session)
        if date > self.backtest_end:
            raise DataError("index daily query cannot exceed backtest end")
        names = self._validate_index_daily_fields(fields)
        sessions = self.calendar.window(date, bar_count, include_end=True)
        source = self._index_daily_frame(index_code, names)
        result = pd.DataFrame(index=sessions)
        result.index.name = "trade_date"
        for field in names:
            column = "close" if field == "price" else field
            result[field] = source[column].reindex(sessions).to_numpy(dtype=float)
        return result

    def _index_weight_dates(self, code: str) -> tuple[str, ...]:
        key = code.upper().strip()
        cached = self._index_constituent_dates.get(key)
        if cached is not None:
            return cached
        if key not in self._index_constituent_codes:
            raise KeyError(f"index constituents are unavailable for {code!r}")
        dates = tuple(
            str(row[0])
            for row in self._connection.execute(
                "SELECT DISTINCT trade_date FROM index_weight "
                "WHERE index_code = ? ORDER BY trade_date",
                [key],
            ).fetchall()
        )
        self._index_constituent_dates[key] = dates
        return dates

    def index_constituents(
        self, index_code: str, session: str | pd.Timestamp
    ) -> pd.DataFrame:
        date = normalize_session(session)
        if date > self.backtest_end:
            raise DataError("index constituent query cannot exceed backtest end")
        code = index_code.upper().strip()
        dates = self._index_weight_dates(code)
        snapshot_position = bisect_left(dates, date.strftime("%Y%m%d")) - 1
        if snapshot_position < 0:
            empty = pd.DataFrame(
                {
                    "asset": pd.Series(dtype=object),
                    "weight": pd.Series(dtype=float),
                    "snapshot_date": pd.Series(dtype="datetime64[ns]"),
                },
                index=pd.Index([], name="ts_code"),
            )
            empty.attrs["index_code"] = code
            return empty
        snapshot = dates[snapshot_position]
        cache_key = (code, snapshot)
        cached = self._index_frames.get(cache_key)
        if cached is None:
            selected = self._connection.execute(
                "SELECT con_code, weight FROM index_weight "
                "WHERE index_code = ? AND trade_date = ? ORDER BY con_code",
                [code, snapshot],
            ).fetchall()
            snapshot_date = pd.to_datetime(snapshot, format="%Y%m%d")
            rows = []
            for con_code, weight in selected:
                try:
                    asset = self.asset_finder.retrieve_asset(str(con_code))
                except LookupError:
                    asset = None
                rows.append(
                    {
                        "ts_code": str(con_code),
                        "asset": asset,
                        "weight": float(weight),
                        "snapshot_date": snapshot_date,
                    }
                )
            cached = pd.DataFrame(rows).set_index("ts_code")[
                ["asset", "weight", "snapshot_date"]
            ]
            cached.attrs.update(
                {
                    "index_code": code,
                    "snapshot_date": snapshot_date,
                    "weight_unit": "percent",
                }
            )
            self._index_frames[cache_key] = cached
            while len(self._index_frames) > _INDEX_FRAME_CACHE_SIZE:
                self._index_frames.popitem(last=False)
        else:
            self._index_frames.move_to_end(cache_key)
        result = cached.copy(deep=True)
        result.attrs = dict(cached.attrs)
        return result

    def benchmark_returns(self, code: str, sessions: pd.DatetimeIndex) -> pd.Series:
        frame = self._connection.execute(
            "SELECT trade_date, close FROM index_daily WHERE ts_code = ? AND trade_date >= ? AND trade_date <= ? ORDER BY trade_date",
            [
                code.upper().strip(),
                sessions[0].strftime("%Y%m%d"),
                sessions[-1].strftime("%Y%m%d"),
            ],
        ).fetchdf()
        if frame.empty:
            raise DataError(f"benchmark {code!r} is unavailable")
        if len(frame) < 2:
            raise DataError(f"benchmark {code!r} has fewer than two price observations")
        prices = pd.Series(
            pd.to_numeric(frame["close"], errors="coerce").to_numpy(dtype=float),
            index=pd.to_datetime(frame["trade_date"].astype(str), format="%Y%m%d"),
            name="benchmark_price",
        ).reindex(sessions)
        return prices.ffill().pct_change(fill_method=None).fillna(0.0)

    def clear_cache(self) -> None:
        for name in (
            "_session_location",
            "_factor_series",
            "factor",
            "raw_bar",
            "is_tradable",
        ):
            method = getattr(self, name, None)
            if method is not None and hasattr(method, "cache_clear"):
                method.cache_clear()
        self._column_cache.clear()
        self._column_cache_bytes = 0
        self._loaded_positions.clear()
        self._categories.clear()
        self._finance_cache.clear()
        self._index_daily_frames.clear()
        self._index_constituent_dates.clear()
        self._index_frames.clear()
        self._bar_presence = None


TushareDataPortal = BundleDataPortal
