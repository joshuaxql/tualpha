"""DuckDB/Parquet point-in-time data portal."""

from __future__ import annotations

import os
from collections import OrderedDict
from collections.abc import Sequence
from functools import lru_cache
from pathlib import Path
from typing import Any, Self

import numpy as np
import pandas as pd

from ..config import AdjustmentMode, normalize_session
from ..exceptions import DataError
from ..model.asset import Asset, AssetFinder
from .bundle.manager import (
    BUNDLE_NAME,
    acquire_bundle_read_lock,
    latest_bundle_path,
    release_bundle_read_lock,
)
from .bundle.parquet_schema import FINANCE_SPECS
from .bundle.parquet_store import load_manifest
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
_DEFAULT_COLUMN_CACHE_MIB = 2048
_INDEX_FRAME_CACHE_SIZE = 128

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
                        asset.list_date.year * 10_000
                        + asset.list_date.month * 100
                        + asset.list_date.day
                    )
                    if asset.list_date is not None
                    else np.iinfo(np.int32).min
                    for asset in self._ordered_assets
                ],
                dtype=np.int32,
            )
            self._delist_date_ints = np.asarray(
                [
                    (
                        asset.delist_date.year * 10_000
                        + asset.delist_date.month * 100
                        + asset.delist_date.day
                    )
                    if asset.delist_date is not None
                    else np.iinfo(np.int32).max
                    for asset in self._ordered_assets
                ],
                dtype=np.int32,
            )
            self._column_cache: OrderedDict[str, np.ndarray] = OrderedDict()
            self._column_cache_bytes = 0
            self._loaded_positions: dict[str, np.ndarray] = {}
            self._categories: dict[str, tuple[str, ...]] = {}
            self._finance_cache: dict[tuple[int, str], pd.DataFrame] = {}
            self._index_arrays: dict[str, pd.DataFrame] = {}
            self._index_frames: OrderedDict[tuple[str, str], pd.DataFrame] = (
                OrderedDict()
            )
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
                cached = np.full(
                    (len(self._sessions), len(self._ordered_assets)),
                    0 if flag else np.nan,
                    dtype=np.uint8 if flag else np.float32,
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
        date = normalize_session(session)
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
        date = normalize_session(session)
        if not asset.is_alive_on(date):
            return np.nan
        session_position = self._sessions.get_indexer([date])[0]
        asset_position = self._asset_position.get(asset.sid, -1)
        if session_position < 0 or asset_position < 0:
            return np.nan
        if "." in field:
            spec = self._extended_daily_spec(field)
            assert spec is not None
            raw = self._full_column(
                spec["role"],
                spec["column"],
                np.asarray([asset_position], dtype=np.int64),
            )[session_position, asset_position]
            if spec["kind"] == "categorical":
                categories = self._categories.get(
                    f"{spec['role']}.{spec['column']}", ()
                )
                code = int(raw)
                return categories[code] if 0 <= code < len(categories) else None
            return float(raw)
        if field not in _BASE_FIELDS:
            raise KeyError(f"unknown daily bar field: {field}")
        role, column = _BASE_STORAGE[field]
        raw = _as_float(
            self._full_column(
                role, column, np.asarray([asset_position], dtype=np.int64)
            )[session_position, asset_position]
        )
        if adjusted and field in _ADJUSTED_PRICE_FIELDS and np.isfinite(raw):
            raw *= self.adjustment_multiplier(
                asset, date, reference_session=reference_session
            )
        return raw

    def prefetch(self, assets: Sequence[Asset], fields: Sequence[str]) -> None:
        """Load fixed asset/field columns once for repeated cross-sectional access."""

        positions = self._asset_positions(assets)
        load_positions = positions[positions >= 0]
        for field in fields:
            if "." in field:
                spec = self._extended_daily_spec(field)
                assert spec is not None
                self._full_column(spec["role"], spec["column"], load_positions)
                continue
            if field not in _BASE_FIELDS:
                raise KeyError(f"unknown daily bar field: {field}")
            role, column = _BASE_STORAGE[field]
            self._full_column(role, column, load_positions)

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
        if needs_adjustment and any(
            field in _ADJUSTED_PRICE_FIELDS for field in fields
        ):
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
                            categories[int(value)]
                            if 0 <= int(value) < len(categories)
                            else None
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

    def history(
        self,
        assets: Sequence[Asset],
        fields: Sequence[str],
        end_session: str | pd.Timestamp,
        bar_count: int,
        *,
        adjusted: bool = True,
    ) -> pd.DataFrame:
        asset_list = list(assets)
        field_list = list(fields)
        sessions = self.calendar.window(end_session, bar_count, include_end=True)
        session_positions, asset_positions, valid = self._matrix_locations(
            asset_list, sessions
        )
        alive = self._alive(asset_positions, sessions)
        adjustments = (
            self._adjustment_matrix(asset_list, sessions, end_session)
            if adjusted and any(field in _ADJUSTED_PRICE_FIELDS for field in field_list)
            else None
        )
        matrices: dict[str, np.ndarray] = {}
        has_strings = False
        for field in field_list:
            if "." in field:
                spec = self._extended_daily_spec(field)
                assert spec is not None
                if spec["kind"] == "categorical":
                    has_strings = True
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
                            categories[int(value)]
                            if 0 <= int(value) < len(categories)
                            else None
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
        columns = pd.MultiIndex.from_product(
            [[asset.ts_code for asset in asset_list], field_list],
            names=["asset", "field"],
        )
        result = pd.DataFrame(
            index=sessions, columns=columns, dtype=object if has_strings else float
        )
        for field, matrix in matrices.items():
            for index, asset in enumerate(asset_list):
                result[(asset.ts_code, field)] = matrix[:, index]
        if has_strings:
            for asset in asset_list:
                for field in field_list:
                    spec = _DAILY_FIELDS.get(field)
                    if spec is None or spec["kind"] != "categorical":
                        result[(asset.ts_code, field)] = pd.to_numeric(
                            result[(asset.ts_code, field)], errors="coerce"
                        ).astype(float)
        return result

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

    def _financial_frame(self, asset: Asset, table: str) -> pd.DataFrame:
        key = (asset.sid, table)
        cached = self._finance_cache.get(key)
        if cached is not None:
            return cached
        spec = FINANCE_SPECS[table]
        selected = ", ".join(f'"{column}"' for column in spec.column_names)
        frame = self._connection.execute(
            f'SELECT {selected} FROM "{table}" WHERE ts_code = ? ORDER BY end_date, effective_ann_date, source_order',
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
        frame = self._financial_frame(asset, table)
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

    def _index_values(self, code: str) -> pd.DataFrame:
        key = code.upper().strip()
        cached = self._index_arrays.get(key)
        if cached is not None:
            return cached
        if key not in self._index_constituent_codes:
            raise KeyError(f"index constituents are unavailable for {code!r}")
        frame = self._connection.execute(
            "SELECT con_code, trade_date, weight FROM index_weight WHERE index_code = ? ORDER BY trade_date, con_code",
            [key],
        ).fetchdf()
        self._index_arrays[key] = frame
        return frame

    def index_constituents(
        self, index_code: str, session: str | pd.Timestamp
    ) -> pd.DataFrame:
        date = normalize_session(session)
        if date > self.backtest_end:
            raise DataError("index constituent query cannot exceed backtest end")
        code = index_code.upper().strip()
        values = self._index_values(code)
        visible_dates = values.loc[
            values["trade_date"].astype(str) < date.strftime("%Y%m%d"), "trade_date"
        ]
        if visible_dates.empty:
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
        snapshot = str(visible_dates.max())
        cache_key = (code, snapshot)
        cached = self._index_frames.get(cache_key)
        if cached is None:
            selected = values[values["trade_date"].astype(str) == snapshot]
            rows = []
            for row in selected.itertuples(index=False):
                try:
                    asset = self.asset_finder.retrieve_asset(str(row.con_code))
                except LookupError:
                    asset = None
                rows.append(
                    {
                        "ts_code": str(row.con_code),
                        "asset": asset,
                        "weight": float(row.weight),
                        "snapshot_date": pd.to_datetime(snapshot, format="%Y%m%d"),
                    }
                )
            cached = pd.DataFrame(rows).set_index("ts_code")[
                ["asset", "weight", "snapshot_date"]
            ]
            cached.attrs.update(
                {
                    "index_code": code,
                    "snapshot_date": pd.to_datetime(snapshot, format="%Y%m%d"),
                    "weight_unit": "percent",
                }
            )
            self._index_frames[cache_key] = cached
            while len(self._index_frames) > _INDEX_FRAME_CACHE_SIZE:
                self._index_frames.popitem(last=False)
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
        self._index_arrays.clear()
        self._index_frames.clear()
        self._bar_presence = None


TushareDataPortal = BundleDataPortal
