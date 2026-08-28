"""Offline factor-research facade built on the point-in-time data portal."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Self

import numpy as np
import pandas as pd

from ..foundation.config import DEFAULT_BUNDLE_ROOT, AdjustmentMode, normalize_session
from ..foundation.exceptions import ConfigurationError, DataError
from ..model.asset import Asset, AssetFinder, AssetType
from .bundle.manager import (
    BUNDLE_NAME,
    acquire_bundle_read_lock,
    release_bundle_read_lock,
)
from .factors import (
    available_operators,
    evaluate_expressions,
    expression_fields,
    expression_window,
    is_factor_expression,
)
from .portal import BundleDataPortal
from .trading_calendar import ChinaTradingCalendar


class FactorData:
    """Fast date-by-asset factor history for offline research.

    The configured universe can be fixed or based on strictly point-in-time
    index snapshots.  Optional ST, listing-age, and suspension filters are
    applied on each signal date, never backfilled from current classifications.
    """

    def __init__(
        self,
        *,
        start: str | pd.Timestamp,
        end: str | pd.Timestamp,
        assets: Iterable[Asset | str] | None = None,
        index_code: str | None = None,
        asset_type: AssetType | str | None = None,
        bundle_root: str | Path = DEFAULT_BUNDLE_ROOT,
        bundle_name: str = BUNDLE_NAME,
        adjustment: AdjustmentMode | str = AdjustmentMode.RAW,
        exclude_st: bool = False,
        min_listed_days: int = 0,
        exclude_suspended: bool = False,
        column_cache_mib: int | None = None,
    ) -> None:
        self.start = normalize_session(start)
        self.end = normalize_session(end)
        if self.start > self.end:
            raise ConfigurationError("factor data start must not be after end")
        if min_listed_days < 0:
            raise ConfigurationError("min_listed_days must not be negative")
        if assets is not None and index_code is not None:
            raise ConfigurationError("assets and index_code are mutually exclusive")
        self.bundle_root = Path(bundle_root).expanduser()
        self.bundle_name = bundle_name
        self.adjustment = AdjustmentMode(adjustment)
        self.index_code = index_code.upper().strip() if index_code else None
        self.exclude_st = bool(exclude_st)
        self.min_listed_days = int(min_listed_days)
        self.exclude_suspended = bool(exclude_suspended)
        self._closed = False
        self._universe_mask_cache: pd.DataFrame | None = None
        self._raw_cache: dict[str, pd.DataFrame] = {}
        self._raw_cache_sessions = pd.DatetimeIndex([])

        lock_key, _ = acquire_bundle_read_lock(self.bundle_root, bundle_name)
        try:
            self.asset_finder = AssetFinder(self.bundle_root, bundle_name)
            self.calendar = ChinaTradingCalendar(self.bundle_root, bundle_name)
            if (
                self.start < self.calendar.first_session
                or self.end > self.calendar.last_session
            ):
                raise ConfigurationError(
                    "factor data range must be within "
                    f"{self.calendar.first_session.date()} and "
                    f"{self.calendar.last_session.date()}"
                )
            self.portal = BundleDataPortal(
                self.bundle_root,
                self.asset_finder,
                self.calendar,
                self.adjustment,
                self.calendar.last_session,
                bundle_name,
                column_cache_max_bytes=(
                    0 if column_cache_mib is None else int(column_cache_mib) * 1024**2
                ),
            )
            self.index_name = self._resolve_index_name()
            self.sessions = self.calendar.sessions_in_range(self.start, self.end)
            if self.sessions.empty:
                raise ConfigurationError(
                    "factor data range contains no trading sessions"
                )
            self.assets = self._resolve_assets(assets, asset_type)
        except Exception:
            portal = getattr(self, "portal", None)
            if portal is not None:
                portal.close()
            raise
        finally:
            release_bundle_read_lock(lock_key)

    def _resolve_index_name(self) -> str | None:
        if self.index_code is None:
            return None
        row = self.portal._connection.execute(
            "SELECT name FROM index_basic WHERE ts_code = ?",
            [self.index_code],
        ).fetchone()
        if row is None or row[0] is None or not str(row[0]).strip():
            return None
        return str(row[0]).strip()

    def _resolve_assets(
        self,
        requested: Iterable[Asset | str] | None,
        asset_type: AssetType | str | None,
    ) -> tuple[Asset, ...]:
        kind = AssetType(asset_type) if asset_type is not None else None
        if requested is not None:
            resolved = [
                value
                if isinstance(value, Asset)
                else self.asset_finder.retrieve_asset(value)
                for value in requested
            ]
        elif self.index_code is not None:
            rows = self.portal._connection.execute(
                "SELECT DISTINCT con_code FROM index_weight "
                "WHERE index_code = ? AND trade_date < ? ORDER BY con_code",
                [self.index_code, self.end.strftime("%Y%m%d")],
            ).fetchall()
            resolved = []
            for row in rows:
                try:
                    resolved.append(self.asset_finder.retrieve_asset(str(row[0])))
                except LookupError:
                    continue
            if not resolved:
                raise DataError(
                    f"index constituents are unavailable for {self.index_code!r} "
                    f"before {self.end.date()}"
                )
        else:
            resolved = list(self.asset_finder.assets(kind))
        if kind is not None:
            resolved = [asset for asset in resolved if asset.asset_type is kind]
        unique = {asset.sid: asset for asset in resolved}
        return tuple(sorted(unique.values(), key=lambda asset: asset.sid))

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()

    def close(self) -> None:
        if not self._closed:
            self.portal.close()
            self._closed = True

    def available_operators(self) -> tuple[str, ...]:
        """Return all supported expression operators."""

        return available_operators()

    def available_fields(self, namespace: str | None = None) -> tuple[str, ...]:
        """Return physical fields available in the underlying Bundle."""

        return self.portal.available_fields(namespace)

    def prefetch(self, expressions: str | Sequence[str]) -> None:
        """Warm a compact date-range/asset-universe cache for factor batches."""

        names = [expressions] if isinstance(expressions, str) else list(expressions)
        raw_fields = list(
            dict.fromkeys(
                [
                    *(name for name in names if not is_factor_expression(name)),
                    *expression_fields(
                        [name for name in names if is_factor_expression(name)]
                    ),
                ]
            )
        )
        requested = raw_fields
        source_sessions = self._source_sessions(names, allow_future=True)
        codes = [asset.ts_code for asset in self.assets]
        cache: dict[str, pd.DataFrame] = {}
        # Load one projected physical field at a time.  This keeps peak memory
        # bounded when the PIT union contains several thousand securities.
        for field in requested:
            raw = self.portal._daily_history(
                self.assets,
                [field],
                source_sessions[-1],
                len(source_sessions),
                adjusted=True,
            )
            cache[field] = (
                raw.xs(field, axis=1, level="field")
                .reindex(index=source_sessions, columns=codes)
                .copy()
            )
        self._raw_cache = cache
        self._raw_cache_sessions = source_sessions

    def _source_sessions(
        self,
        expressions: Sequence[str],
        *,
        allow_future: bool,
    ) -> pd.DatetimeIndex:
        factor_expressions = [
            name for name in expressions if is_factor_expression(name)
        ]
        lookback, lookahead = expression_window(factor_expressions)
        first = int(self.calendar.sessions.searchsorted(self.sessions[0]))
        last = int(self.calendar.sessions.searchsorted(self.sessions[-1]))
        source_first = max(0, first - lookback)
        source_last = min(
            len(self.calendar.sessions) - 1,
            last + (lookahead if allow_future else 0),
        )
        return self.calendar.sessions[source_first : source_last + 1]

    def _index_membership_mask(self) -> pd.DataFrame:
        codes = [asset.ts_code for asset in self.assets]
        output = np.zeros((len(self.sessions), len(codes)), dtype=bool)
        if self.index_code is None or not len(self.sessions):
            output[:] = True
            return pd.DataFrame(output, index=self.sessions, columns=codes)
        rows = self.portal._connection.execute(
            "SELECT trade_date, con_code FROM index_weight "
            "WHERE index_code = ? AND trade_date < ? "
            "ORDER BY trade_date, con_code",
            [self.index_code, self.sessions[-1].strftime("%Y%m%d")],
        ).fetchall()
        by_snapshot: dict[str, list[str]] = {}
        for trade_date, code in rows:
            by_snapshot.setdefault(str(trade_date), []).append(str(code))
        snapshots = np.asarray(sorted(by_snapshot), dtype=object)
        if not len(snapshots):
            return pd.DataFrame(output, index=self.sessions, columns=codes)
        date_values = self.sessions.strftime("%Y%m%d").to_numpy(dtype=object)
        selected = np.searchsorted(snapshots, date_values, side="left") - 1
        code_positions = {code: position for position, code in enumerate(codes)}
        for snapshot_position in np.unique(selected[selected >= 0]):
            snapshot = str(snapshots[int(snapshot_position)])
            columns = [
                code_positions[code]
                for code in by_snapshot[snapshot]
                if code in code_positions
            ]
            if columns:
                output[np.ix_(selected == snapshot_position, columns)] = True
        return pd.DataFrame(output, index=self.sessions, columns=codes)

    def universe_mask(self) -> pd.DataFrame:
        """Return the date-by-asset inclusion mask used by :meth:`history`."""

        if self._universe_mask_cache is not None:
            return self._universe_mask_cache.copy()
        mask = self._index_membership_mask()
        for asset in self.assets:
            values = np.ones(len(self.sessions), dtype=bool)
            if asset.list_date is not None:
                eligible = asset.list_date + pd.Timedelta(days=self.min_listed_days)
                values &= self.sessions >= eligible
            if asset.delist_date is not None:
                values &= self.sessions <= asset.delist_date
            mask.loc[:, asset.ts_code] &= values
        filter_fields = []
        if self.exclude_st:
            filter_fields.append("stock_st.is_st")
        if self.exclude_suspended:
            filter_fields.append("suspended")
        if filter_fields:
            cached_filters = set(filter_fields).issubset(self._raw_cache)
            filters = (
                None
                if cached_filters
                else self.portal.factor_history(
                    self.assets,
                    filter_fields,
                    self.start,
                    self.end,
                    adjusted=False,
                )
            )
            if self.exclude_st:
                values = (
                    self._raw_cache["stock_st.is_st"].reindex(self.sessions)
                    if cached_filters
                    else filters.xs("stock_st.is_st", axis=1, level="field")
                )
                mask &= values.fillna(0.0).ne(1.0)
            if self.exclude_suspended:
                values = (
                    self._raw_cache["suspended"].reindex(self.sessions)
                    if cached_filters
                    else filters.xs("suspended", axis=1, level="field")
                )
                mask &= values.fillna(0.0).eq(0.0)
        self._universe_mask_cache = mask.astype(bool)
        return mask.copy()

    def history(
        self,
        expressions: str | Sequence[str],
        *,
        allow_future: bool = False,
    ) -> pd.DataFrame:
        """Calculate factor expressions over the configured range and universe.

        A single expression returns a date-by-code DataFrame.  Multiple
        expressions return the standard ``(asset, field)`` column layout.
        ``allow_future`` exists for analysis labels such as
        ``FUTURE_RETURNS`` and should never be used to construct a signal.
        """

        if self._closed:
            raise DataError("factor data session is closed")
        one_expression = isinstance(expressions, str)
        names = [expressions] if one_expression else list(expressions)
        arrays = self.history_arrays(names, allow_future=allow_future)
        matrices = {name: values.to_numpy() for name, values in arrays.items()}
        result = self.portal._history_frame(
            self.assets,
            names,
            self.sessions,
            matrices,
        )
        if one_expression:
            single = result.xs(names[0], axis=1, level="field")
            single.columns.name = None
            return single
        return result

    def history_arrays(
        self,
        expressions: str | Sequence[str],
        *,
        allow_future: bool = False,
    ) -> dict[str, pd.DataFrame]:
        """Return expression-keyed matrices without assembling MultiIndex columns.

        This is the memory-bounded interface for performance benchmarks and
        streaming factor pipelines.  :meth:`history` uses it internally before
        assembling the conventional public DataFrame layout.
        """

        if self._closed:
            raise DataError("factor data session is closed")
        names = [expressions] if isinstance(expressions, str) else list(expressions)
        factor_expressions = [name for name in names if is_factor_expression(name)]
        raw_fields = list(
            dict.fromkeys(
                [
                    *(name for name in names if name not in factor_expressions),
                    *expression_fields(factor_expressions),
                ]
            )
        )
        if set(raw_fields).issubset(self._raw_cache):
            source_sessions = self._source_sessions(names, allow_future=allow_future)
            inputs = {
                field: self._raw_cache[field].reindex(source_sessions)
                for field in raw_fields
            }
            calculated = (
                evaluate_expressions(factor_expressions, inputs)
                if factor_expressions
                else {}
            )
            output = {
                name: calculated.get(name, inputs.get(name)).reindex(self.sessions)
                for name in names
            }
        else:
            result = self.portal.factor_history(
                self.assets,
                names,
                self.start,
                self.end,
                adjusted=True,
                allow_future=allow_future,
            )
            output = {name: result.xs(name, axis=1, level="field") for name in names}
        mask = self.universe_mask()
        return {name: values.where(mask) for name, values in output.items()}


def factor_data(**kwargs: object) -> FactorData:
    """Create a context-manageable :class:`FactorData` research session."""

    return FactorData(**kwargs)


__all__ = ["FactorData", "factor_data"]
