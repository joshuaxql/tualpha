"""Schema-7 HDF5 point-in-time data portal."""

from __future__ import annotations

import json
import os
from collections import OrderedDict
from collections.abc import Sequence
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Self

import h5py
import numpy as np
import pandas as pd

from ._hdf5_store import (
    HDF5_FILES,
    PACKED_LAYOUT,
    date_to_int,
    dates_to_int,
    load_assets_manifest,
    open_h5_checked,
)
from .assets import Asset, AssetFinder
from .bundle import (
    BUNDLE_NAME,
    acquire_bundle_read_lock,
    latest_bundle_path,
    release_bundle_read_lock,
)
from .calendar import ChinaTradingCalendar
from .config import AdjustmentMode, normalize_session
from .exceptions import DataError

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
_FINANCIAL_NAMESPACES = frozenset(
    {"balancesheet", "income", "cashflow", "fina_indicator"}
)
_DEFAULT_COLUMN_CACHE_MIB = 2048
_QUERY_PLAN_CACHE_SIZE = 128
_INDEX_FRAME_CACHE_SIZE = 128


def _default_column_cache_bytes() -> int:
    raw = os.environ.get("TUALPHA_COLUMN_CACHE_MIB", str(_DEFAULT_COLUMN_CACHE_MIB))
    try:
        mib = int(raw)
    except ValueError as exc:
        raise DataError("TUALPHA_COLUMN_CACHE_MIB must be an integer") from exc
    if mib < 0:
        raise DataError("TUALPHA_COLUMN_CACHE_MIB must not be negative")
    return mib * 1024**2


@dataclass(slots=True)
class _AssetQueryPlan:
    sids: np.ndarray
    first: np.ndarray
    last: np.ndarray
    offsets: np.ndarray
    list_dates: np.ndarray
    delist_dates: np.ndarray


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


def _as_float(value: object, default: float = np.nan) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if np.isfinite(result) else default


def _field_array(dataset: h5py.Dataset, field: str) -> np.ndarray:
    values = np.asarray(dataset.fields(field)[:])
    if values.dtype.names:
        values = np.asarray(values[field])
    return values


def _decode_bytes(value: object) -> str:
    if isinstance(value, bytes):
        return value.rstrip(b"\0").decode("utf-8")
    if isinstance(value, np.bytes_):
        return bytes(value).rstrip(b"\0").decode("utf-8")
    return str(value)


class BundleDataPortal:
    """Read bars and point-in-time datasets from an immutable HDF5 Bundle."""

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
        from .data import DailyBar

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
        self._bundle_lock_key, self._bundle_lock = acquire_bundle_read_lock(
            self.bundle_root, bundle_name
        )
        self._h5: dict[str, h5py.File] = {}
        self._finance = None
        self._daily_reader = None
        self._daily_table = None
        self._index_table = None
        self._index_constituents = None
        try:
            self.bundle_path = latest_bundle_path(self.bundle_root, bundle_name)
            self._manifest = load_assets_manifest(self.bundle_path / "assets.pk")
            generation = str(self._manifest["generation"])
            if (
                asset_finder.bundle_generation != generation
                or calendar.bundle_generation != generation
            ):
                raise DataError(
                    "bundle components come from different generations; "
                    "recreate AssetFinder and ChinaTradingCalendar"
                )
            self._h5 = {
                role: open_h5_checked(
                    self.bundle_path / filename,
                    expected_role=role,
                    expected_generation=generation,
                )
                for filename, role in HDF5_FILES.items()
            }
            self._assets_by_sid = {
                asset.sid: asset
                for asset in sorted(asset_finder, key=lambda item: item.sid)
            }
            self._ordered_assets = tuple(self._assets_by_sid.values())
            self._initialize_row_metadata()
            self._column_cache: OrderedDict[str, np.ndarray] = OrderedDict()
            self._column_cache_bytes = 0
            self._query_plans: OrderedDict[tuple[int, ...], _AssetQueryPlan] = (
                OrderedDict()
            )
            self._index_arrays: dict[str, np.ndarray] = {}
            self._index_frames: OrderedDict[tuple[str, int], pd.DataFrame] = (
                OrderedDict()
            )
            self._bar_presence: np.ndarray | None = None
            self._daily_fields = self._load_daily_fields()
            self._financial_fields = self._load_financial_fields()
            self._index_constituent_codes = tuple(
                str(code).upper() for code in self._manifest.get("index_codes", [])
            )
        except Exception:
            for handle in self._h5.values():
                handle.close()
            self._h5 = {}
            release_bundle_read_lock(self._bundle_lock_key)
            self._bundle_lock_key = None
            raise

    def _initialize_row_metadata(self) -> None:
        max_sid = max(self._assets_by_sid, default=-1)
        size = max_sid + 1
        self._first_row_by_sid = np.full(size, -1, dtype=np.int64)
        self._last_row_by_sid = np.full(size, -1, dtype=np.int64)
        self._calendar_offset_by_sid = np.full(size, -1, dtype=np.int64)
        self._list_date_by_sid = np.full(size, np.iinfo(np.int64).min, dtype=np.int64)
        self._delist_date_by_sid = np.full(size, np.iinfo(np.int64).max, dtype=np.int64)
        self._packed_slices: dict[int, slice] = {}
        position = 0
        daily = self._h5["daily"]["data"]
        sessions = self.calendar.sessions
        for asset in self._ordered_assets:
            dataset = daily.get(asset.ts_code)
            if dataset is None or asset.list_date is None or asset.delist_date is None:
                continue
            offset = int(sessions.searchsorted(asset.list_date, side="left"))
            stop = int(sessions.searchsorted(asset.delist_date, side="right"))
            length = max(0, stop - offset)
            if not length:
                continue
            if len(dataset) != length:
                raise DataError(
                    f"daily.h5 rows do not match assets.pk for {asset.ts_code}"
                )
            first = position
            last = position + length - 1
            self._first_row_by_sid[asset.sid] = first
            self._last_row_by_sid[asset.sid] = last
            self._calendar_offset_by_sid[asset.sid] = offset
            self._packed_slices[asset.sid] = slice(first, last + 1)
            position = last + 1
            if asset.list_date is not None:
                self._list_date_by_sid[asset.sid] = asset.list_date.value
            if asset.delist_date is not None:
                self._delist_date_by_sid[asset.sid] = asset.delist_date.value
        self._packed_row_count = position

    def _load_daily_fields(self) -> dict[str, dict[str, Any]]:
        fields: dict[str, dict[str, Any]] = {}
        for namespace in _EXTENDED_DAILY_NAMESPACES:
            raw = self._h5[namespace].attrs.get("fields", "{}")
            try:
                specs = json.loads(str(raw))
            except ValueError as exc:
                raise DataError(f"invalid field registry in {namespace}.h5") from exc
            for column, spec in specs.items():
                categories: list[str] = []
                if spec["kind"] == "categorical":
                    dictionary = self._h5[namespace].get("dictionary")
                    if dictionary is None or column not in dictionary:
                        raise DataError(
                            f"categorical dictionary is missing: {namespace}.{column}"
                        )
                    categories = [str(value) for value in dictionary[column].asstr()[:]]
                fields[f"{namespace}.{column}"] = {
                    "role": namespace,
                    "column": str(column),
                    "kind": str(spec["kind"]),
                    "categories": categories,
                }
        return fields

    def _load_financial_fields(self) -> dict[str, tuple[str, str]]:
        raw = self._h5["finance"].attrs.get("fields", "{}")
        try:
            registry = json.loads(str(raw))
        except ValueError as exc:
            raise DataError("invalid field registry in finance.h5") from exc
        fields: dict[str, tuple[str, str]] = {}
        for table, columns in registry.items():
            for column in columns:
                fields[f"{table}.{column}"] = (str(table), str(column))
        return fields

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
        spec = self._daily_fields.get(field)
        if spec is None:
            raise KeyError(f"unknown daily field: {field!r}")
        return spec

    def _financial_spec(self, field: str) -> tuple[str, str]:
        try:
            return self._financial_fields[field]
        except KeyError as exc:
            if "." not in field:
                raise KeyError(
                    f"financial field must use '<dataset>.<column>': {field!r}"
                ) from exc
            raise KeyError(f"unknown financial value field: {field!r}") from exc

    def available_fields(self, namespace: str | None = None) -> tuple[str, ...]:
        namespaces = (
            sorted(_EXTENDED_DAILY_NAMESPACES | _FINANCIAL_NAMESPACES)
            if namespace is None
            else [namespace]
        )
        fields: list[str] = []
        for name in namespaces:
            if name in _EXTENDED_DAILY_NAMESPACES:
                fields.extend(
                    field
                    for field in self._daily_fields
                    if field.startswith(f"{name}.")
                )
            elif name in _FINANCIAL_NAMESPACES:
                fields.extend(
                    field
                    for field in self._financial_fields
                    if field.startswith(f"{name}.")
                )
            else:
                raise KeyError(f"unknown data namespace: {name!r}")
        return tuple(sorted(fields))

    def close(self) -> None:
        self.clear_cache()
        for handle in self._h5.values():
            handle.close()
        self._h5 = {}
        if getattr(self, "_bundle_lock_key", None) is not None:
            release_bundle_read_lock(self._bundle_lock_key)
            self._bundle_lock_key = None

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()

    @lru_cache(maxsize=8_192)  # noqa: B019
    def _session_location(self, date_value: int) -> int | None:
        date = pd.to_datetime(str(date_value), format="%Y%m%d")
        try:
            return int(self.calendar.sessions.get_loc(date))
        except (KeyError, TypeError, ValueError):
            return None

    @lru_cache(maxsize=131_072)  # noqa: B019
    def _row_index(self, sid: int, date_value: int) -> int | None:
        day_location = self._session_location(date_value)
        if day_location is None or sid < 0 or sid >= len(self._first_row_by_sid):
            return None
        first = int(self._first_row_by_sid[sid])
        offset = int(self._calendar_offset_by_sid[sid])
        if first < 0 or offset < 0:
            return None
        row = first + day_location - offset
        if row < first or row > int(self._last_row_by_sid[sid]):
            return None
        return row

    def _query_plan(self, assets: Sequence[Asset]) -> _AssetQueryPlan:
        key = tuple(asset.sid for asset in assets)
        cached = self._query_plans.get(key)
        if cached is not None:
            self._query_plans.move_to_end(key)
            return cached

        sids = np.asarray(key, dtype=np.int64)
        known = (sids >= 0) & (sids < len(self._first_row_by_sid))
        first = np.full(len(sids), -1, dtype=np.int64)
        last = np.full(len(sids), -1, dtype=np.int64)
        offsets = np.full(len(sids), -1, dtype=np.int64)
        list_dates = np.full(len(sids), np.iinfo(np.int64).min, dtype=np.int64)
        delist_dates = np.full(len(sids), np.iinfo(np.int64).max, dtype=np.int64)
        first[known] = self._first_row_by_sid[sids[known]]
        last[known] = self._last_row_by_sid[sids[known]]
        offsets[known] = self._calendar_offset_by_sid[sids[known]]
        list_dates[known] = self._list_date_by_sid[sids[known]]
        delist_dates[known] = self._delist_date_by_sid[sids[known]]
        for values in (sids, first, last, offsets, list_dates, delist_dates):
            values.setflags(write=False)
        plan = _AssetQueryPlan(
            sids=sids,
            first=first,
            last=last,
            offsets=offsets,
            list_dates=list_dates,
            delist_dates=delist_dates,
        )
        self._query_plans[key] = plan
        self._query_plans.move_to_end(key)
        while len(self._query_plans) > _QUERY_PLAN_CACHE_SIZE:
            self._query_plans.popitem(last=False)
        return plan

    def _row_indices(
        self,
        assets: Sequence[Asset],
        session: str | pd.Timestamp,
    ) -> tuple[pd.Timestamp, np.ndarray, np.ndarray, np.ndarray]:
        date = normalize_session(session)
        plan = self._query_plan(assets)
        if not len(plan.sids):
            rows = np.empty(0, dtype=np.int64)
            empty = np.zeros(0, dtype=bool)
            return date, rows, empty, empty
        alive = (plan.list_dates <= date.value) & (date.value <= plan.delist_dates)
        day_location = self._session_location(date_to_int(date))
        if day_location is None:
            rows = np.full(len(plan.sids), -1, dtype=np.int64)
            return date, rows, np.zeros(len(plan.sids), dtype=bool), alive
        rows = plan.first + day_location - plan.offsets
        valid = (
            alive
            & (plan.first >= 0)
            & (plan.offsets >= 0)
            & (rows >= plan.first)
            & (rows <= plan.last)
        )
        rows = np.asarray(rows, dtype=np.int64)
        rows[~valid] = -1
        return date, rows, valid, alive

    def _row_matrix(
        self,
        assets: Sequence[Asset],
        sessions: pd.DatetimeIndex,
    ) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        plan = self._query_plan(assets)
        shape = (len(sessions), len(plan.sids))
        if not shape[0] or not shape[1]:
            return (
                np.full(shape, -1, dtype=np.int64),
                np.zeros(shape, dtype=bool),
                np.zeros(shape, dtype=bool),
            )
        locations = self.calendar.sessions.get_indexer(sessions).astype(np.int64)
        session_values = sessions.asi8[:, None]
        alive = (plan.list_dates[None, :] <= session_values) & (
            session_values <= plan.delist_dates[None, :]
        )
        rows = plan.first[None, :] + locations[:, None] - plan.offsets[None, :]
        valid = (
            alive
            & (locations[:, None] >= 0)
            & (plan.first[None, :] >= 0)
            & (plan.offsets[None, :] >= 0)
            & (rows >= plan.first[None, :])
            & (rows <= plan.last[None, :])
        )
        rows = np.asarray(rows, dtype=np.int64)
        rows[~valid] = -1
        return rows, valid, alive

    def _full_column(self, role: str, field: str) -> np.ndarray:
        key = f"{role}.{field}"
        cached = self._column_cache.get(key)
        if cached is not None:
            self._column_cache.move_to_end(key)
            return cached
        handle = self._h5[role]
        packed = handle.get("packed")
        if isinstance(packed, h5py.Group) and field in packed:
            if str(packed.attrs.get("layout", "")) != PACKED_LAYOUT:
                raise DataError(f"packed layout is invalid in {role}.h5")
            dataset = packed[field]
            if len(dataset) != self._packed_row_count:
                raise DataError(f"packed rows do not align in {role}.h5: {field}")
            values = np.asarray(dataset[:])
        else:
            group = handle["data"]
            dtype: np.dtype[Any] | None = None
            for asset in self._ordered_assets:
                if asset.ts_code in group:
                    dtype = group[asset.ts_code].dtype[field]
                    break
            if dtype is None:
                return np.array([], dtype=float)
            values = np.empty(self._packed_row_count, dtype=dtype)
            if dtype.kind == "f":
                values.fill(np.nan)
            elif dtype.kind == "i":
                values.fill(-1)
            else:
                values.fill(0)
            for asset in self._ordered_assets:
                target = self._packed_slices.get(asset.sid)
                if target is None:
                    continue
                dataset = group.get(asset.ts_code)
                if dataset is None or field not in (dataset.dtype.names or ()):
                    continue
                source = _field_array(dataset, field)
                if len(source) != target.stop - target.start:
                    raise DataError(
                        f"{role}.h5 rows do not align with daily.h5 for {asset.ts_code}"
                    )
                values[target] = source
        values.setflags(write=False)
        if values.nbytes <= self._column_cache_max_bytes:
            while (
                self._column_cache
                and self._column_cache_bytes + values.nbytes
                > self._column_cache_max_bytes
            ):
                _, evicted = self._column_cache.popitem(last=False)
                self._column_cache_bytes -= evicted.nbytes
            self._column_cache[key] = values
            self._column_cache_bytes += values.nbytes
        return values

    def _column_values(self, role: str, field: str, rows: np.ndarray) -> np.ndarray:
        if not len(rows):
            return np.array([], dtype=float)
        return np.asarray(self._full_column(role, field)[rows])

    def _bar_presence_array(self) -> np.ndarray:
        if self._bar_presence is None:
            close = np.asarray(self._full_column("daily", "close"), dtype=float)
            suspended = np.asarray(
                self._full_column("suspend_d", "suspended"), dtype=bool
            )
            present = np.isfinite(close) | suspended
            present.setflags(write=False)
            self._bar_presence = present
        return self._bar_presence

    def _scalar(
        self, asset: Asset, session: pd.Timestamp, role: str, field: str
    ) -> object:
        row = self._row_index(asset.sid, date_to_int(session))
        if row is None:
            return np.nan
        return self._full_column(role, field)[row]

    @lru_cache(maxsize=16_384)  # noqa: B019
    def _factor_series(self, sid: int) -> tuple[np.ndarray, np.ndarray]:
        asset = self._assets_by_sid.get(sid)
        if asset is None:
            return np.array([], dtype=np.int32), np.array([], dtype=float)
        dataset = self._h5["adj_factor"]["data"].get(asset.ts_code)
        if dataset is None:
            return np.array([], dtype=np.int32), np.array([], dtype=float)
        dates = _field_array(dataset, "trade_date").astype(np.int32, copy=False)
        factors = _field_array(dataset, "adj_factor").astype(float, copy=False)
        valid = np.isfinite(factors) & (factors > 0)
        return dates[valid], factors[valid]

    @lru_cache(maxsize=131_072)  # noqa: B019
    def factor(self, asset: Asset, session: pd.Timestamp) -> float:
        dates, factors = self._factor_series(asset.sid)
        if not len(dates):
            return 1.0
        index = int(np.searchsorted(dates, date_to_int(session), side="right") - 1)
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

    @lru_cache(maxsize=131_072)  # noqa: B019
    def raw_bar(self, asset: Asset, session: str | pd.Timestamp):
        date = normalize_session(session)
        if not asset.is_alive_on(date):
            return None
        row = self._row_index(asset.sid, date_to_int(date))
        if row is None:
            return None
        values = {
            field: _as_float(self._full_column("daily", field)[row])
            for field in (
                "open",
                "high",
                "low",
                "close",
                "pre_close",
                "volume",
                "turnover",
            )
        }
        up = _as_float(self._full_column("stk_limit", "up_limit")[row])
        down = _as_float(self._full_column("stk_limit", "down_limit")[row])
        suspended = bool(self._full_column("suspend_d", "suspended")[row])
        if (
            not any(
                np.isfinite(values[field]) for field in ("open", "high", "low", "close")
            )
            and not suspended
        ):
            return None
        return self._daily_bar_type(
            asset=asset,
            session=date,
            open=values["open"],
            high=values["high"],
            low=values["low"],
            close=values["close"],
            pre_close=values["pre_close"],
            volume=max(0.0, _as_float(values["volume"], 0.0)),
            turnover=max(0.0, _as_float(values["turnover"], 0.0)),
            up_limit=up,
            down_limit=down,
            suspended=suspended,
        )

    def execution_bars(
        self,
        assets: Sequence[Asset],
        session: str | pd.Timestamp,
        execution_field: str,
    ) -> dict[Asset, Any]:
        if execution_field not in {"open", "close"}:
            raise ValueError("execution_field must be open or close")
        asset_list = list(assets)
        date, rows, valid, _ = self._row_indices(asset_list, session)
        positions = np.flatnonzero(valid)
        if positions.size:
            present = self._bar_presence_array()[rows[positions]]
            valid[positions[~present]] = False
        positions = np.flatnonzero(valid)
        result = {asset: None for asset in asset_list}
        if not positions.size:
            return result
        selected = rows[positions]
        prices = self._column_values("daily", execution_field, selected).astype(float)
        volumes = np.maximum(
            self._column_values("daily", "volume", selected).astype(float), 0.0
        )
        up = self._column_values("stk_limit", "up_limit", selected).astype(float)
        down = self._column_values("stk_limit", "down_limit", selected).astype(float)
        suspended = self._column_values("suspend_d", "suspended", selected).astype(bool)
        for offset, asset_position in enumerate(positions):
            price = float(prices[offset])
            result[asset_list[asset_position]] = self._daily_bar_type(
                asset=asset_list[asset_position],
                session=date,
                open=price if execution_field == "open" else np.nan,
                high=np.nan,
                low=np.nan,
                close=price if execution_field == "close" else np.nan,
                pre_close=np.nan,
                volume=float(volumes[offset]),
                turnover=0.0,
                up_limit=_as_float(up[offset]),
                down_limit=_as_float(down[offset]),
                suspended=bool(suspended[offset]),
            )
        return result

    @lru_cache(maxsize=131_072)  # noqa: B019
    def is_tradable(self, asset: Asset, session: str | pd.Timestamp) -> bool:
        date = normalize_session(session)
        row = self._row_index(asset.sid, date_to_int(date))
        if (
            row is None
            or not asset.is_alive_on(date)
            or not self._bar_presence_array()[row]
        ):
            return False
        volume = _as_float(self._full_column("daily", "volume")[row], 0.0)
        suspended = bool(self._full_column("suspend_d", "suspended")[row])
        return not suspended and volume > 0

    def _extended_value(
        self, asset: Asset, session: pd.Timestamp, field: str
    ) -> object:
        spec = self._extended_daily_spec(field)
        assert spec is not None
        is_string = spec["kind"] == "categorical"
        if not asset.is_alive_on(session):
            return None if is_string else np.nan
        row = self._row_index(asset.sid, date_to_int(session))
        if row is None:
            return 0.0 if field == "stock_st.is_st" else None if is_string else np.nan
        value = self._full_column(spec["role"], spec["column"])[row]
        if is_string:
            code = int(value)
            categories = spec["categories"]
            return categories[code] if 0 <= code < len(categories) else None
        if spec["kind"] == "flag":
            return float(value)
        return _as_float(value)

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
        if "." in field:
            return self._extended_value(asset, date, field)
        if field not in _BASE_FIELDS:
            raise KeyError(f"unknown daily bar field: {field}")
        if not asset.is_alive_on(date):
            return np.nan
        role, column = _BASE_STORAGE[field]
        value = _as_float(self._scalar(asset, date, role, column))
        if adjusted and field in _ADJUSTED_PRICE_FIELDS and np.isfinite(value):
            value *= self.adjustment_multiplier(
                asset, date, reference_session=reference_session
            )
        return value

    def values(
        self,
        assets: Sequence[Asset],
        session: str | pd.Timestamp,
        fields: Sequence[str],
        *,
        adjusted: bool = True,
        reference_session: str | pd.Timestamp | None = None,
    ) -> dict[str, np.ndarray]:
        asset_list = list(assets)
        date, rows, valid, alive = self._row_indices(asset_list, session)
        needs_adjustment = adjusted and not (
            self.adjustment is AdjustmentMode.RAW
            or (self.adjustment is AdjustmentMode.QFQ and reference_session is None)
        )
        multipliers: np.ndarray | None = None
        if needs_adjustment and any(
            field in _ADJUSTED_PRICE_FIELDS for field in fields
        ):
            reference = date if reference_session is None else reference_session
            multipliers = self._adjustment_matrix(
                asset_list, pd.DatetimeIndex([date]), reference
            )[0]
        output: dict[str, np.ndarray] = {}
        for field in fields:
            if "." in field:
                spec = self._extended_daily_spec(field)
                assert spec is not None
                if spec["kind"] == "categorical":
                    values = np.full(len(asset_list), None, dtype=object)
                elif field == "stock_st.is_st":
                    values = np.zeros(len(asset_list), dtype=float)
                    values[~alive] = np.nan
                else:
                    values = np.full(len(asset_list), np.nan, dtype=float)
                positions = np.flatnonzero(valid)
                raw = self._column_values(spec["role"], spec["column"], rows[positions])
                if spec["kind"] == "categorical":
                    categories = spec["categories"]
                    values[positions] = [
                        categories[int(value)]
                        if 0 <= int(value) < len(categories)
                        else None
                        for value in raw
                    ]
                else:
                    values[positions] = np.asarray(raw, dtype=float)
                output[field] = values
                continue
            if field not in _BASE_FIELDS:
                raise KeyError(f"unknown daily bar field: {field}")
            values = np.full(len(asset_list), np.nan, dtype=float)
            positions = np.flatnonzero(valid)
            if positions.size:
                role, column = _BASE_STORAGE[field]
                decoded = self._column_values(role, column, rows[positions]).astype(
                    float
                )
                if field in {"volume", "turnover"}:
                    decoded = np.maximum(decoded, 0.0)
                values[positions] = decoded
                if multipliers is not None and field in _ADJUSTED_PRICE_FIELDS:
                    values[positions] *= multipliers[positions]
            output[field] = values
        return output

    def _adjustment_matrix(
        self,
        assets: Sequence[Asset],
        sessions: pd.DatetimeIndex,
        reference_session: str | pd.Timestamp,
    ) -> np.ndarray:
        multipliers = np.ones((len(sessions), len(assets)), dtype=float)
        if self.adjustment is AdjustmentMode.RAW or not len(sessions):
            return multipliers
        session_dates = dates_to_int(sessions)
        reference = date_to_int(reference_session)
        for asset_index, asset in enumerate(assets):
            factor_dates, factor_values = self._factor_series(asset.sid)
            if not len(factor_dates):
                continue
            indices = np.searchsorted(factor_dates, session_dates, side="right") - 1
            visible = indices >= 0
            factors = np.ones(len(sessions), dtype=float)
            factors[visible] = factor_values[indices[visible]]
            if self.adjustment is AdjustmentMode.HFQ:
                multipliers[:, asset_index] = factors
                continue
            reference_index = (
                int(np.searchsorted(factor_dates, reference, side="right")) - 1
            )
            reference_factor = (
                float(factor_values[reference_index]) if reference_index >= 0 else 1.0
            )
            if reference_factor:
                multipliers[:, asset_index] = factors / reference_factor
        return multipliers

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
        rows, valid, alive = self._row_matrix(asset_list, sessions)
        extended = {
            field: self._extended_daily_spec(field)
            for field in field_list
            if "." in field
        }
        has_strings = any(
            spec is not None and spec["kind"] == "categorical"
            for spec in extended.values()
        )
        adjustments = (
            self._adjustment_matrix(asset_list, sessions, end_session)
            if adjusted and any(field in _ADJUSTED_PRICE_FIELDS for field in field_list)
            else None
        )
        matrices: dict[str, np.ndarray] = {}
        for field in field_list:
            spec = extended.get(field)
            if spec is not None:
                if spec["kind"] == "categorical":
                    values = np.full(rows.shape, None, dtype=object)
                elif field == "stock_st.is_st":
                    values = np.zeros(rows.shape, dtype=float)
                    values[~alive] = np.nan
                else:
                    values = np.full(rows.shape, np.nan, dtype=float)
                if valid.any():
                    raw = self._full_column(spec["role"], spec["column"])[rows[valid]]
                    if spec["kind"] == "categorical":
                        categories = spec["categories"]
                        values[valid] = [
                            categories[int(value)]
                            if 0 <= int(value) < len(categories)
                            else None
                            for value in raw
                        ]
                    else:
                        values[valid] = np.asarray(raw, dtype=float)
                matrices[field] = values
                continue
            if field not in _BASE_FIELDS:
                raise KeyError(f"unknown daily bar field: {field}")
            values = np.full(rows.shape, np.nan, dtype=float)
            if valid.any():
                role, column = _BASE_STORAGE[field]
                decoded = self._full_column(role, column)[rows[valid]].astype(float)
                if field in {"volume", "turnover"}:
                    decoded = np.maximum(decoded, 0.0)
                values[valid] = decoded
                if adjustments is not None and field in _ADJUSTED_PRICE_FIELDS:
                    values[valid] *= adjustments[valid]
            matrices[field] = values

        columns = pd.MultiIndex.from_product(
            [[asset.ts_code for asset in asset_list], field_list],
            names=["asset", "field"],
        )
        result = pd.DataFrame(
            index=sessions,
            columns=columns,
            dtype=object if has_strings else float,
        )
        for field, matrix in matrices.items():
            for asset_index, asset in enumerate(asset_list):
                result[(asset.ts_code, field)] = matrix[:, asset_index]
        if has_strings:
            for asset in asset_list:
                for field in field_list:
                    spec = extended.get(field)
                    if spec is None or spec["kind"] != "categorical":
                        result[(asset.ts_code, field)] = pd.to_numeric(
                            result[(asset.ts_code, field)], errors="coerce"
                        ).astype(float)
        return result

    def _financial_array(self, asset: Asset, table: str) -> np.ndarray:
        group = self._h5["finance"]["data"].get(table)
        if group is None or asset.ts_code not in group:
            return np.array([], dtype=[])
        return np.asarray(group[asset.ts_code][:])

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
        if not asset.is_alive_on(date):
            return pd.DataFrame(columns=[field for field, _ in fields])
        values = self._financial_array(asset, table)
        if not len(values):
            return pd.DataFrame(columns=[field for field, _ in fields])
        date_int = date_to_int(date)
        mask = (values["effective_ann_date"] < date_int) & (
            values["end_date"] <= date_int
        )
        if table != "fina_indicator" and report_type is not None:
            reports = np.array(
                [_decode_bytes(value) for value in values["report_type"]]
            )
            mask &= reports == str(report_type)
        if end_date is not None:
            mask &= values["end_date"] == date_to_int(end_date)
        visible = values[mask]
        if not len(visible):
            return pd.DataFrame(columns=[field for field, _ in fields])
        frame = pd.DataFrame(
            {
                "end_date": visible["end_date"],
                "effective_ann_date": visible["effective_ann_date"],
                "update_rank": np.array(
                    [
                        1 if _decode_bytes(value) == "1" else 0
                        for value in visible["update_flag"]
                    ]
                ),
                "source_order": visible["source_order"],
                **{field: visible[column] for field, column in fields},
            }
        )
        frame = (
            frame.sort_values(
                ["end_date", "effective_ann_date", "update_rank", "source_order"],
                ascending=[False, False, False, False],
            )
            .drop_duplicates("end_date", keep="first")
            .head(periods)
        )
        frame["end_date"] = pd.to_datetime(
            frame["end_date"].astype(str), format="%Y%m%d"
        )
        return frame.set_index("end_date")[[field for field, _ in fields]]

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

    def _index_values(self, code: str) -> np.ndarray:
        cached = self._index_arrays.get(code)
        if cached is not None:
            return cached
        group = self._h5["index_weight"]["data"]
        if code not in group:
            raise KeyError(f"index constituents are unavailable for {code!r}")
        values = np.asarray(group[code][:])
        values.setflags(write=False)
        self._index_arrays[code] = values
        return values

    @lru_cache(maxsize=32_768)  # noqa: B019
    def _index_rows(
        self, index_code: str, session_int: int
    ) -> tuple[tuple[str, int | None, float, int], ...]:
        code = index_code.upper().strip()
        values = self._index_values(code)
        dates = values["snapshot_date"]
        position = int(np.searchsorted(dates, session_int, side="left") - 1)
        if position < 0:
            return ()
        snapshot = int(dates[position])
        selected = values[dates == snapshot]
        return tuple(
            (
                _decode_bytes(row["con_code"]),
                int(row["sid"]) if int(row["sid"]) >= 0 else None,
                float(row["weight"]),
                snapshot,
            )
            for row in selected
        )

    def index_constituents(
        self, index_code: str, session: str | pd.Timestamp
    ) -> pd.DataFrame:
        date = normalize_session(session)
        if date > self.backtest_end:
            raise DataError("index constituent query cannot exceed backtest end")
        code = index_code.upper().strip()
        rows = self._index_rows(code, date_to_int(date))
        if not rows:
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

        snapshot = rows[0][3]
        cache_key = (code, snapshot)
        cached = self._index_frames.get(cache_key)
        if cached is None:
            result_rows = []
            for con_code, sid, weight, _ in rows:
                asset = None
                if sid is not None:
                    try:
                        asset = self.asset_finder.retrieve_asset(sid)
                    except LookupError:
                        pass
                result_rows.append(
                    {
                        "ts_code": con_code,
                        "asset": asset,
                        "weight": weight,
                        "snapshot_date": pd.to_datetime(str(snapshot), format="%Y%m%d"),
                    }
                )
            cached = pd.DataFrame(result_rows).set_index("ts_code")
            cached.attrs.update(
                {
                    "index_code": code,
                    "snapshot_date": cached["snapshot_date"].iloc[0],
                    "weight_unit": "percent",
                }
            )
            cached = cached[["asset", "weight", "snapshot_date"]]
            self._index_frames[cache_key] = cached
            self._index_frames.move_to_end(cache_key)
            while len(self._index_frames) > _INDEX_FRAME_CACHE_SIZE:
                self._index_frames.popitem(last=False)
        else:
            self._index_frames.move_to_end(cache_key)
        result = cached.copy(deep=True)
        result.attrs = dict(cached.attrs)
        return result

    def benchmark_returns(self, code: str, sessions: pd.DatetimeIndex) -> pd.Series:
        key = code.upper().strip()
        group = self._h5["daily"]["data"]
        if key not in group:
            raise DataError(f"benchmark {code!r} is unavailable")
        dataset = group[key]
        dates = pd.to_datetime(
            _field_array(dataset, "trade_date").astype(str), format="%Y%m%d"
        )
        closes = _field_array(dataset, "close").astype(float)
        prices = pd.Series(closes, index=dates, name="benchmark_price").reindex(
            sessions
        )
        if prices.notna().sum() < 2:
            raise DataError(f"benchmark {code!r} has fewer than two price observations")
        return prices.ffill().pct_change(fill_method=None).fillna(0.0)

    def clear_cache(self) -> None:
        for name in (
            "_session_location",
            "_row_index",
            "_factor_series",
            "factor",
            "raw_bar",
            "is_tradable",
            "_index_rows",
        ):
            method = getattr(self, name, None)
            if method is not None and hasattr(method, "cache_clear"):
                method.cache_clear()
        if getattr(self, "_column_cache", None) is not None:
            self._column_cache.clear()
            self._column_cache_bytes = 0
        if getattr(self, "_query_plans", None) is not None:
            self._query_plans.clear()
        if getattr(self, "_index_arrays", None) is not None:
            self._index_arrays.clear()
        if getattr(self, "_index_frames", None) is not None:
            self._index_frames.clear()
        self._bar_presence = None
