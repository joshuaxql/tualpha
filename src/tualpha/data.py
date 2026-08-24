"""Fast point-in-time access to the latest Zipline bundle."""

from __future__ import annotations

import json
import re
import sqlite3
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Self

import bcolz
import numpy as np
import pandas as pd
from zipline.data.bar_reader import NoDataAfterDate, NoDataBeforeDate, NoDataOnDate
from zipline.data.bcolz_daily_bars import BcolzDailyBarReader

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

_PRICE_FIELDS = {
    "open",
    "high",
    "low",
    "close",
    "price",
    "pre_close",
    "up_limit",
    "down_limit",
}
_DAILY_EXTENSION_FIELDS = {
    "pre_close",
    "turnover",
    "volume",
    "up_limit",
    "down_limit",
    "adj_factor",
    "suspended",
}
_EXTENDED_DAILY_NAMESPACES = frozenset(
    {"daily_basic", "moneyflow", "industry", "stock_st"}
)
_FINANCIAL_NAMESPACES = frozenset(
    {"balancesheet", "income", "cashflow", "fina_indicator"}
)
_FINANCIAL_METADATA_COLUMNS = frozenset(
    {
        "sid",
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
)
_IDENTIFIER_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


def _sql_identifier(name: str) -> str:
    if not _IDENTIFIER_PATTERN.fullmatch(name):
        raise KeyError(f"invalid data field: {name!r}")
    return f'"{name}"'


def _is_string_type(data_type: str) -> bool:
    upper = data_type.upper()
    return "CHAR" in upper or upper in {"VARCHAR", "TEXT"}


def _as_float(value: object, default: float = np.nan) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if np.isfinite(result) else default


def _epoch_seconds(session: str | pd.Timestamp) -> int:
    return int(normalize_session(session).value // 1_000_000_000)


@dataclass(frozen=True, slots=True)
class DailyBar:
    """Raw exchange-price bar and A-share trading constraints."""

    asset: Asset
    session: pd.Timestamp
    open: float
    high: float
    low: float
    close: float
    pre_close: float
    volume: float
    turnover: float
    up_limit: float = np.nan
    down_limit: float = np.nan
    suspended: bool = False

    def value(self, field: str) -> float:
        name = "close" if field == "price" else field
        if not hasattr(self, name):
            raise KeyError(f"unknown daily bar field: {field}")
        return float(getattr(self, name))


class BundleDataPortal:
    """Read OHLCV and daily extensions from Bcolz and finance from SQLite."""

    def __init__(
        self,
        bundle_root: str | Path,
        asset_finder: AssetFinder,
        calendar: ChinaTradingCalendar,
        adjustment: AdjustmentMode | str,
        backtest_end: str | pd.Timestamp,
        bundle_name: str = BUNDLE_NAME,
    ) -> None:
        self.bundle_root = Path(bundle_root).expanduser()
        self.asset_finder = asset_finder
        self.calendar = calendar
        self.adjustment = AdjustmentMode(adjustment)
        self.backtest_end = normalize_session(backtest_end)
        self._bundle_lock_key, self._bundle_lock = acquire_bundle_read_lock(
            self.bundle_root, bundle_name
        )
        try:
            self.bundle_path = latest_bundle_path(self.bundle_root, bundle_name)
            manifest = json.loads(
                (self.bundle_path / "manifest.json").read_text(encoding="utf-8")
            )
            generation = str(manifest["generated_at"])
            if (
                asset_finder.bundle_generation != generation
                or calendar.bundle_generation != generation
            ):
                raise DataError(
                    "bundle components come from different generations; "
                    "recreate AssetFinder and ChinaTradingCalendar"
                )
            daily_path = self.bundle_path / "daily_equities.bcolz"
            index_path = self.bundle_path / "index_daily.bcolz"
            finance_path = self.bundle_path / "finance.sqlite"
            if (
                not daily_path.is_dir()
                or not index_path.is_dir()
                or not finance_path.is_file()
            ):
                raise DataError(f"bundle is incomplete: {self.bundle_path}")
            self._daily_table = bcolz.open(rootdir=str(daily_path), mode="r")
            self._daily_reader = BcolzDailyBarReader(self._daily_table)
            self._daily_fields = dict(self._daily_table.attrs["tualpha_fields"])
            self._index_table = bcolz.open(rootdir=str(index_path), mode="r")
            finance_uri = f"file:{finance_path.resolve().as_posix()}?mode=ro"
            self._finance = sqlite3.connect(finance_uri, uri=True)
            self._finance.execute("PRAGMA query_only = ON")
            self._financial_columns = self._load_financial_columns()
            self._benchmark_sids = {
                str(code).upper(): int(sid)
                for code, sid in manifest.get("benchmark_sids", {}).items()
            }
        except Exception:
            if getattr(self, "_finance", None) is not None:
                self._finance.close()
                self._finance = None
            release_bundle_read_lock(self._bundle_lock_key)
            self._bundle_lock_key = None
            raise

    def _load_financial_columns(self) -> dict[str, dict[str, str]]:
        result: dict[str, dict[str, str]] = {}
        for namespace in _FINANCIAL_NAMESPACES:
            table = f"financial_{namespace}"
            rows = self._finance.execute(
                f"PRAGMA table_info({_sql_identifier(table)})"
            ).fetchall()
            result[table] = {str(row[1]): str(row[2]) for row in rows}
        return result

    def _extended_daily_spec(self, field: str) -> tuple[str, str, bool] | None:
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
        return namespace, str(spec["column"]), spec["kind"] == "categorical"

    def _financial_spec(self, field: str) -> tuple[str, str]:
        if "." not in field:
            raise KeyError(f"financial field must use '<dataset>.<column>': {field!r}")
        namespace, column = field.split(".", 1)
        if namespace not in _FINANCIAL_NAMESPACES:
            raise KeyError(f"unknown financial namespace: {namespace!r}")
        table = f"financial_{namespace}"
        columns = self._financial_columns.get(table, {})
        if column in _FINANCIAL_METADATA_COLUMNS or column not in columns:
            raise KeyError(f"unknown financial value field: {field!r}")
        if _is_string_type(columns[column]):
            raise KeyError(f"financial value field is not numeric: {field!r}")
        return table, column

    def available_fields(self, namespace: str | None = None) -> tuple[str, ...]:
        """List namespaced daily and financial fields in this bundle."""

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
                columns = self._financial_columns.get(f"financial_{name}", {})
                fields.extend(
                    f"{name}.{column}"
                    for column, data_type in columns.items()
                    if column not in _FINANCIAL_METADATA_COLUMNS
                    and not _is_string_type(data_type)
                )
            else:
                raise KeyError(f"unknown data namespace: {name!r}")
        return tuple(sorted(fields))

    def close(self) -> None:
        if getattr(self, "_finance", None) is not None:
            self._finance.close()
            self._finance = None
        if getattr(self, "_bundle_lock_key", None) is not None:
            release_bundle_read_lock(self._bundle_lock_key)
            self._bundle_lock_key = None

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()

    @lru_cache(maxsize=131_072)  # noqa: B019 - cache lifetime equals this portal
    def _row_index(self, sid: int, epoch: int) -> int | None:
        key = str(sid)
        first_rows = self._daily_table.attrs["first_row"]
        last_rows = self._daily_table.attrs["last_row"]
        if key not in first_rows or key not in last_rows:
            return None
        first = int(first_rows[key])
        last = int(last_rows[key])
        days = np.asarray(self._daily_table["day"][first : last + 1], dtype=np.int64)
        position = int(np.searchsorted(days, epoch))
        if position >= len(days) or int(days[position]) != epoch:
            return None
        return first + position

    def _extension_scalar(self, row: int, field: str) -> object:
        spec = self._daily_fields[field]
        value = self._daily_table[str(spec["column"])][row]
        if spec["kind"] == "categorical":
            code = int(value)
            categories = spec["categories"]
            return str(categories[code]) if 0 <= code < len(categories) else None
        return _as_float(value, 0.0 if spec["kind"] == "flag" else np.nan)

    @lru_cache(maxsize=131_072)  # noqa: B019 - cache lifetime equals this portal
    def _metadata_row(self, sid: int, epoch: int) -> tuple[object, ...] | None:
        row = self._row_index(sid, epoch)
        if row is None:
            return None
        return tuple(
            self._extension_scalar(row, field)
            for field in (
                "pre_close",
                "turnover",
                "volume",
                "up_limit",
                "down_limit",
                "adj_factor",
                "suspended",
            )
        )

    @lru_cache(maxsize=16_384)  # noqa: B019 - cache lifetime equals this portal
    def _factor_series(self, sid: int) -> tuple[np.ndarray, np.ndarray]:
        key = str(sid)
        first_rows = self._daily_table.attrs["first_row"]
        last_rows = self._daily_table.attrs["last_row"]
        if key not in first_rows or key not in last_rows:
            return np.array([], dtype=np.int64), np.array([], dtype=float)
        first = int(first_rows[key])
        last = int(last_rows[key]) + 1
        epochs = np.asarray(self._daily_table["day"][first:last], dtype=np.int64)
        column = self._daily_fields["adj_factor"]["column"]
        factors = np.asarray(self._daily_table[str(column)][first:last], dtype=float)
        valid = np.isfinite(factors) & (factors > 0)
        return epochs[valid], factors[valid]

    @lru_cache(maxsize=131_072)  # noqa: B019 - cache lifetime equals this portal
    def factor(self, asset: Asset, session: pd.Timestamp) -> float:
        epochs, factors = self._factor_series(asset.sid)
        if not len(epochs):
            return 1.0
        index = int(np.searchsorted(epochs, _epoch_seconds(session), side="right") - 1)
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

    def _reader_value(self, asset: Asset, session: pd.Timestamp, field: str) -> float:
        name = "close" if field == "price" else field
        try:
            value = self._daily_reader.get_value(asset.sid, session, name)
        except (NoDataAfterDate, NoDataBeforeDate, NoDataOnDate, KeyError, ValueError):
            return np.nan
        return _as_float(value)

    def raw_bar(self, asset: Asset, session: str | pd.Timestamp) -> DailyBar | None:
        date = normalize_session(session)
        if not asset.is_alive_on(date):
            return None
        metadata = self._metadata_row(asset.sid, _epoch_seconds(date))
        values = {
            field: self._reader_value(asset, date, field)
            for field in ("open", "high", "low", "close")
        }
        suspended = bool(metadata[6]) if metadata is not None else False
        has_price = any(
            np.isfinite(values[field]) for field in ("open", "high", "low", "close")
        )
        if not has_price and not suspended:
            return None
        return DailyBar(
            asset=asset,
            session=date,
            open=values["open"],
            high=values["high"],
            low=values["low"],
            close=values["close"],
            volume=(
                max(0.0, _as_float(metadata[2], 0.0)) if metadata is not None else 0.0
            ),
            pre_close=_as_float(metadata[0]) if metadata is not None else np.nan,
            turnover=max(0.0, _as_float(metadata[1], 0.0))
            if metadata is not None
            else 0.0,
            up_limit=_as_float(metadata[3]) if metadata is not None else np.nan,
            down_limit=_as_float(metadata[4]) if metadata is not None else np.nan,
            suspended=suspended,
        )

    def _extended_daily_value(
        self,
        asset: Asset,
        session: str | pd.Timestamp,
        field: str,
    ) -> object:
        spec = self._extended_daily_spec(field)
        if spec is None:
            raise KeyError(f"unknown daily field: {field!r}")
        _, _, is_string = spec
        date = normalize_session(session)
        if not asset.is_alive_on(date):
            return None if is_string else np.nan
        row = self._row_index(asset.sid, _epoch_seconds(date))
        if row is None:
            return 0.0 if field == "stock_st.is_st" else None if is_string else np.nan
        value = self._extension_scalar(row, field)
        if value is None:
            return 0.0 if field == "stock_st.is_st" else None if is_string else np.nan
        return value

    def value(
        self,
        asset: Asset,
        session: str | pd.Timestamp,
        field: str,
        *,
        adjusted: bool = True,
        reference_session: str | pd.Timestamp | None = None,
    ) -> object:
        if "." in field:
            return self._extended_daily_value(asset, session, field)
        bar = self.raw_bar(asset, session)
        if bar is None:
            return np.nan
        value = bar.value(field)
        if adjusted and field in _PRICE_FIELDS and np.isfinite(value):
            value *= self.adjustment_multiplier(
                asset, session, reference_session=reference_session
            )
        return value

    def _daily_extension_arrays(
        self,
        assets: Sequence[Asset],
        sessions: pd.DatetimeIndex,
        fields: Sequence[str],
    ) -> dict[str, np.ndarray]:
        arrays: dict[str, np.ndarray] = {}
        for field in fields:
            kind = self._daily_fields[field]["kind"]
            if kind == "categorical":
                arrays[field] = np.full(
                    (len(sessions), len(assets)), None, dtype=object
                )
            elif kind == "flag":
                arrays[field] = np.zeros((len(sessions), len(assets)), dtype=float)
            else:
                arrays[field] = np.full(
                    (len(sessions), len(assets)), np.nan, dtype=float
                )
        if not fields or not sessions.size or not assets:
            return arrays

        requested = np.asarray(
            [_epoch_seconds(session) for session in sessions], dtype=np.int64
        )
        first_rows = self._daily_table.attrs["first_row"]
        last_rows = self._daily_table.attrs["last_row"]
        for asset_index, asset in enumerate(assets):
            key = str(asset.sid)
            if key not in first_rows or key not in last_rows:
                continue
            first = int(first_rows[key])
            last = int(last_rows[key]) + 1
            days = np.asarray(self._daily_table["day"][first:last], dtype=np.int64)
            positions = np.searchsorted(days, requested)
            valid = positions < len(days)
            valid[valid] &= days[positions[valid]] == requested[valid]
            output_positions = np.flatnonzero(valid)
            source_positions = positions[valid]
            for field in fields:
                spec = self._daily_fields[field]
                raw = np.asarray(self._daily_table[str(spec["column"])][first:last])[
                    source_positions
                ]
                if spec["kind"] == "categorical":
                    categories = spec["categories"]
                    values = [
                        str(categories[int(value)])
                        if 0 <= int(value) < len(categories)
                        else None
                        for value in raw
                    ]
                else:
                    values = np.asarray(raw, dtype=float)
                arrays[field][output_positions, asset_index] = values
        return arrays

    def history(
        self,
        assets: Sequence[Asset],
        fields: Sequence[str],
        end_session: str | pd.Timestamp,
        bar_count: int,
        *,
        adjusted: bool = True,
    ) -> pd.DataFrame:
        sessions = self.calendar.window(end_session, bar_count, include_end=True)
        extended_specs = {
            field: spec
            for field in fields
            if (spec := self._extended_daily_spec(field)) is not None
        }
        columns = pd.MultiIndex.from_product(
            [[asset.ts_code for asset in assets], list(fields)],
            names=["asset", "field"],
        )
        has_strings = any(spec[2] for spec in extended_specs.values())
        result = pd.DataFrame(
            index=sessions,
            columns=columns,
            dtype=object if has_strings else float,
        )
        if sessions.empty or not assets:
            return result

        reader_fields = sorted(
            {"close" if field == "price" else field for field in fields}.intersection(
                {"open", "high", "low", "close"}
            )
        )
        reader_arrays: dict[str, np.ndarray] = {}
        if reader_fields:
            loaded = self._daily_reader.load_raw_arrays(
                reader_fields,
                sessions[0],
                sessions[-1],
                [asset.sid for asset in assets],
            )
            reader_arrays = dict(zip(reader_fields, loaded, strict=True))

        extension_fields = [
            field
            for field in fields
            if field in _DAILY_EXTENSION_FIELDS or field in extended_specs
        ]
        extension_arrays = self._daily_extension_arrays(
            assets, sessions, extension_fields
        )
        for asset_index, asset in enumerate(assets):
            multipliers = np.array(
                [
                    self.adjustment_multiplier(
                        asset, session, reference_session=end_session
                    )
                    for session in sessions
                ],
                dtype=float,
            )
            for field in fields:
                source = "close" if field == "price" else field
                if source in reader_arrays:
                    values = reader_arrays[source][:, asset_index].astype(float)
                elif field in extension_arrays:
                    values = extension_arrays[field][:, asset_index]
                else:
                    raise KeyError(f"unknown history field: {field}")
                if adjusted and field in _PRICE_FIELDS:
                    values = values * multipliers
                result[(asset.ts_code, field)] = values
        if has_strings:
            for asset in assets:
                for field in fields:
                    spec = extended_specs.get(field)
                    if spec is None or not spec[2]:
                        column = (asset.ts_code, field)
                        result[column] = pd.to_numeric(
                            result[column], errors="coerce"
                        ).astype(float)
        return result

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
        conditions = [
            "sid = ?",
            "effective_ann_date < ?",
            "end_date <= ?",
        ]
        date_text = date.strftime("%Y-%m-%d")
        parameters: list[object] = [asset.sid, date_text, date_text]
        if table != "financial_fina_indicator" and report_type is not None:
            conditions.append("report_type = ?")
            parameters.append(str(report_type))
        if end_date is not None:
            conditions.append("end_date = ?")
            parameters.append(end_date.strftime("%Y-%m-%d"))
        selected = ", ".join(_sql_identifier(column) for _, column in fields)
        parameters.append(periods)
        cursor = self._finance.execute(
            f"""
            WITH visible AS (
                SELECT * FROM {_sql_identifier(table)}
                WHERE {" AND ".join(conditions)}
            ), ranked AS (
                SELECT *, row_number() OVER (
                    PARTITION BY end_date
                    ORDER BY effective_ann_date DESC,
                             CASE WHEN update_flag = '1' THEN 1 ELSE 0 END DESC,
                             source_order DESC
                ) AS revision_rank
                FROM visible
            )
            SELECT end_date, {selected}
            FROM ranked
            WHERE revision_rank = 1
            ORDER BY end_date DESC
            LIMIT ?
            """,
            parameters,
        )
        frame = pd.DataFrame(
            cursor.fetchall(),
            columns=[description[0] for description in cursor.description],
        )
        if frame.empty:
            return pd.DataFrame(columns=[field for field, _ in fields])
        frame["end_date"] = pd.to_datetime(frame["end_date"])
        frame = frame.set_index("end_date")
        frame.index.name = "end_date"
        return frame.rename(columns={column: field for field, column in fields})

    def fundamental(
        self,
        asset: Asset,
        session: str | pd.Timestamp,
        field: str,
        *,
        period: str | pd.Timestamp = "latest",
        report_type: str | None = "1",
    ) -> float:
        """Return one financial value visible on ``session`` without lookahead."""

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
        if frame.empty:
            return np.nan
        return _as_float(frame.iloc[0][field])

    def fundamentals(
        self,
        asset: Asset,
        session: str | pd.Timestamp,
        fields: str | Sequence[str],
        *,
        periods: int = 4,
        report_type: str | None = "1",
    ) -> pd.DataFrame:
        """Return latest visible revisions for multiple report periods."""

        if periods <= 0:
            raise ValueError("periods must be positive")
        field_names = [fields] if isinstance(fields, str) else list(fields)
        if not field_names:
            return pd.DataFrame(index=pd.DatetimeIndex([], name="end_date"))
        grouped: dict[str, list[tuple[str, str]]] = {}
        for field in field_names:
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
                index=pd.DatetimeIndex([], name="end_date"), columns=field_names
            )
        result = pd.concat(nonempty, axis=1).sort_index(ascending=False)
        result = result.loc[~result.index.duplicated(keep="first")].head(periods)
        return result.reindex(columns=field_names)

    def benchmark_returns(self, code: str, sessions: pd.DatetimeIndex) -> pd.Series:
        try:
            asset = self.asset_finder.retrieve_asset(code)
        except LookupError:
            asset = None
        if asset is not None:
            frame = self.history(
                [asset], ["close"], sessions[-1], len(sessions), adjusted=True
            )
            prices = frame[(asset.ts_code, "close")].reindex(sessions)
        else:
            sid = self._benchmark_sids.get(code.upper())
            if sid is None:
                prices = pd.Series(index=sessions, dtype=float)
            else:
                key = str(sid)
                first_rows = self._index_table.attrs["first_row"]
                last_rows = self._index_table.attrs["last_row"]
                if key not in first_rows or key not in last_rows:
                    prices = pd.Series(index=sessions, dtype=float)
                else:
                    first = int(first_rows[key])
                    last = int(last_rows[key]) + 1
                    days = np.asarray(
                        self._index_table["day"][first:last], dtype=np.int64
                    )
                    closes = np.asarray(
                        self._index_table["close"][first:last], dtype=float
                    )
                    prices = pd.Series(
                        closes,
                        index=pd.to_datetime(days, unit="s"),
                        name="benchmark_price",
                    ).reindex(sessions)
        if prices.notna().sum() < 2:
            raise DataError(f"benchmark {code!r} has fewer than two price observations")
        return prices.ffill().pct_change(fill_method=None).fillna(0.0)

    def clear_cache(self) -> None:
        self._row_index.cache_clear()
        self._metadata_row.cache_clear()
        self._factor_series.cache_clear()
        self.factor.cache_clear()


# Backward-compatible internal name; all reads now come from the bundle.
TushareDataPortal = BundleDataPortal


class BarData:
    """A callback-scoped, point-in-time data facade similar to Zipline BarData."""

    def __init__(self, portal: BundleDataPortal) -> None:
        self._portal = portal
        self._session: pd.Timestamp | None = None

    @property
    def current_session(self) -> pd.Timestamp:
        if self._session is None:
            raise RuntimeError("BarData is not bound to a trading session")
        return self._session

    def _set_session(self, session: str | pd.Timestamp) -> None:
        self._session = normalize_session(session)

    @staticmethod
    def _asset_list(value: Asset | Iterable[Asset]) -> tuple[list[Asset], bool]:
        if isinstance(value, Asset):
            return [value], True
        return list(value), False

    @staticmethod
    def _field_list(value: str | Iterable[str]) -> tuple[list[str], bool]:
        if isinstance(value, str):
            return [value], True
        return list(value), False

    def current(
        self, assets: Asset | Iterable[Asset], fields: str | Iterable[str]
    ) -> object | pd.Series | pd.DataFrame:
        asset_list, one_asset = self._asset_list(assets)
        field_list, one_field = self._field_list(fields)
        values = {
            (asset.ts_code, field): self._portal.value(
                asset, self.current_session, field, adjusted=True
            )
            for asset in asset_list
            for field in field_list
        }
        if one_asset and one_field:
            return values[(asset_list[0].ts_code, field_list[0])]
        if one_asset:
            return pd.Series(
                {field: values[(asset_list[0].ts_code, field)] for field in field_list},
                name=asset_list[0].ts_code,
            )
        if one_field:
            return pd.Series(
                {
                    asset.ts_code: values[(asset.ts_code, field_list[0])]
                    for asset in asset_list
                },
                name=field_list[0],
            )
        return pd.DataFrame(
            {
                asset.ts_code: {
                    field: values[(asset.ts_code, field)] for field in field_list
                }
                for asset in asset_list
            }
        ).T

    def raw_current(self, asset: Asset, field: str) -> object:
        return self._portal.value(asset, self.current_session, field, adjusted=False)

    def history(
        self,
        assets: Asset | Iterable[Asset],
        fields: str | Iterable[str],
        bar_count: int,
    ) -> pd.Series | pd.DataFrame:
        asset_list, one_asset = self._asset_list(assets)
        field_list, one_field = self._field_list(fields)
        frame = self._portal.history(
            asset_list,
            field_list,
            self.current_session,
            bar_count,
            adjusted=True,
        )
        if one_asset and one_field:
            result = frame[(asset_list[0].ts_code, field_list[0])]
            result.name = asset_list[0].ts_code
            return result
        if one_field:
            return frame.xs(field_list[0], axis=1, level="field")
        if one_asset:
            return frame.xs(asset_list[0].ts_code, axis=1, level="asset")
        return frame

    def fundamental(
        self,
        asset: Asset,
        field: str,
        *,
        period: str | pd.Timestamp = "latest",
        report_type: str | None = "1",
    ) -> float:
        return self._portal.fundamental(
            asset,
            self.current_session,
            field,
            period=period,
            report_type=report_type,
        )

    def fundamentals(
        self,
        asset: Asset,
        fields: str | Iterable[str],
        *,
        periods: int = 4,
        report_type: str | None = "1",
    ) -> pd.DataFrame:
        field_list, _ = self._field_list(fields)
        return self._portal.fundamentals(
            asset,
            self.current_session,
            field_list,
            periods=periods,
            report_type=report_type,
        )

    def available_fields(self, namespace: str | None = None) -> tuple[str, ...]:
        return self._portal.available_fields(namespace)

    def can_trade(self, asset: Asset) -> bool:
        bar = self._portal.raw_bar(asset, self.current_session)
        return bool(bar is not None and not bar.suspended and bar.volume > 0)
