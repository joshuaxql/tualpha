"""Tushare-backed trading-calendar normalization and session operations."""

from __future__ import annotations

import re
from pathlib import Path

import numpy as np
import pandas as pd

from ._hdf5_store import ints_to_dates, load_assets_manifest, load_trade_dates
from .exceptions import DataError

CALENDAR_NAME = "XSHG"
CALENDAR_SOURCE = "tushare.trade_cal"
CALENDAR_EXCHANGE = "SSE"
_DATE_PATTERN = re.compile(r"\d{8}\Z")


def _normalize_session(value: object) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is not None:
        timestamp = timestamp.tz_convert("Asia/Shanghai").tz_localize(None)
    return timestamp.normalize()


class SessionCalendar:
    """Lightweight ordered-session calendar used by Bundle writers and readers."""

    def __init__(
        self,
        sessions: pd.DatetimeIndex,
        name: str = CALENDAR_NAME,
    ) -> None:
        normalized = pd.DatetimeIndex(pd.to_datetime(sessions)).normalize()
        if normalized.tz is not None:
            normalized = normalized.tz_convert("Asia/Shanghai").tz_localize(None)
        if normalized.empty:
            raise DataError("trading calendar contains no open sessions")
        values = normalized.asi8
        if len(np.unique(values)) != len(values) or np.any(values[1:] <= values[:-1]):
            raise DataError("trading calendar sessions must be strictly increasing")
        self.name = str(name)
        self._sessions = normalized
        self._positions = {
            session: position for position, session in enumerate(self._sessions)
        }

    @property
    def sessions(self) -> pd.DatetimeIndex:
        return self._sessions

    @property
    def first_session(self) -> pd.Timestamp:
        return self._sessions[0]

    @property
    def last_session(self) -> pd.Timestamp:
        return self._sessions[-1]

    def is_session(self, value: object) -> bool:
        return _normalize_session(value) in self._positions

    def sessions_in_range(self, start: object, end: object) -> pd.DatetimeIndex:
        start_date = _normalize_session(start)
        end_date = _normalize_session(end)
        return self._sessions[
            (self._sessions >= start_date) & (self._sessions <= end_date)
        ]

    def previous_session(self, value: object) -> pd.Timestamp:
        date = _normalize_session(value)
        index = int(self._sessions.searchsorted(date, side="left") - 1)
        if index < 0:
            raise IndexError(f"no trading session before {date.date()}")
        return self._sessions[index]

    def next_session(self, value: object) -> pd.Timestamp:
        date = _normalize_session(value)
        index = int(self._sessions.searchsorted(date, side="right"))
        if index >= len(self._sessions):
            raise IndexError(f"no trading session after {date.date()}")
        return self._sessions[index]

    def session_offset(self, value: object, offset: int) -> pd.Timestamp:
        date = _normalize_session(value)
        position = self._positions.get(date)
        if position is None:
            raise IndexError(f"not a trading session: {date.date()}")
        target = position + int(offset)
        if target < 0 or target >= len(self._sessions):
            raise IndexError(f"calendar offset {offset} is outside stored sessions")
        return self._sessions[target]

    def window(
        self,
        end: object,
        count: int,
        include_end: bool = True,
    ) -> pd.DatetimeIndex:
        if count <= 0:
            raise ValueError("count must be positive")
        date = _normalize_session(end)
        right = int(
            self._sessions.searchsorted(date, side="right" if include_end else "left")
        )
        return self._sessions[max(0, right - count) : right]


def normalize_trade_calendar(frame: pd.DataFrame) -> pd.DataFrame:
    """Validate and normalize a complete Tushare SSE trade_cal frame."""

    required = {"exchange", "cal_date", "is_open"}
    missing = required.difference(frame.columns)
    if missing:
        raise DataError(f"trade_cal is missing required columns: {sorted(missing)}")
    columns = ["exchange", "cal_date", "is_open"]
    if "pretrade_date" in frame.columns:
        columns.append("pretrade_date")
    normalized = frame.loc[:, columns].copy()
    for column in columns:
        normalized[column] = normalized[column].fillna("").astype(str).str.strip()
    normalized["exchange"] = normalized["exchange"].str.upper()
    normalized = normalized[normalized["exchange"].eq(CALENDAR_EXCHANGE)].copy()
    if normalized.empty:
        raise DataError("trade_cal contains no SSE rows")
    invalid_dates = ~normalized["cal_date"].map(
        lambda value: bool(_DATE_PATTERN.fullmatch(value))
    )
    if invalid_dates.any():
        values = normalized.loc[invalid_dates, "cal_date"].unique().tolist()
        raise DataError(f"trade_cal contains invalid cal_date values: {values[:5]}")
    parsed_dates = pd.to_datetime(
        normalized["cal_date"], format="%Y%m%d", errors="coerce"
    )
    if parsed_dates.isna().any():
        values = normalized.loc[parsed_dates.isna(), "cal_date"].unique().tolist()
        raise DataError(f"trade_cal contains impossible dates: {values[:5]}")
    invalid_open = ~normalized["is_open"].isin(["0", "1"])
    if invalid_open.any():
        values = normalized.loc[invalid_open, "is_open"].unique().tolist()
        raise DataError(f"trade_cal contains invalid is_open values: {values[:5]}")
    if "pretrade_date" not in normalized:
        normalized["pretrade_date"] = ""
    invalid_pretrade = normalized["pretrade_date"].map(
        lambda value: bool(value) and not bool(_DATE_PATTERN.fullmatch(value))
    )
    if invalid_pretrade.any():
        values = normalized.loc[invalid_pretrade, "pretrade_date"].unique().tolist()
        raise DataError(
            f"trade_cal contains invalid pretrade_date values: {values[:5]}"
        )

    rows: list[dict[str, str]] = []
    for cal_date, group in normalized.groupby("cal_date", sort=True):
        open_values = group["is_open"].unique().tolist()
        if len(open_values) != 1:
            raise DataError(f"trade_cal has conflicting is_open rows for {cal_date}")
        previous_values = sorted(
            {value for value in group["pretrade_date"].tolist() if value}
        )
        if len(previous_values) > 1:
            raise DataError(
                f"trade_cal has conflicting pretrade_date rows for {cal_date}"
            )
        rows.append(
            {
                "exchange": CALENDAR_EXCHANGE,
                "cal_date": str(cal_date),
                "is_open": str(open_values[0]),
                "pretrade_date": previous_values[0] if previous_values else "",
            }
        )
    output = pd.DataFrame(rows)
    natural_dates = pd.date_range(
        pd.Timestamp(output.iloc[0]["cal_date"]),
        pd.Timestamp(output.iloc[-1]["cal_date"]),
        freq="D",
    ).strftime("%Y%m%d")
    missing_natural_dates = pd.Index(natural_dates).difference(output["cal_date"])
    if len(missing_natural_dates):
        raise DataError(
            f"trade_cal has missing natural dates: {missing_natural_dates[:5].tolist()}"
        )
    open_rows = output[output["is_open"].eq("1")]
    previous_open: str | None = None
    for row in open_rows.itertuples(index=False):
        if (
            previous_open is not None
            and row.pretrade_date
            and row.pretrade_date != previous_open
        ):
            raise DataError(
                f"trade_cal pretrade_date mismatch for {row.cal_date}: "
                f"expected {previous_open}, got {row.pretrade_date}"
            )
        previous_open = row.cal_date
    return output.reset_index(drop=True)


def sessions_from_trade_calendar(
    frame: pd.DataFrame,
    start_session: object,
    end_session: object,
) -> pd.DatetimeIndex:
    """Select authoritative open SSE sessions for a Bundle date range."""

    start = _normalize_session(start_session)
    end = _normalize_session(end_session)
    start_text = start.strftime("%Y%m%d")
    end_text = end.strftime("%Y%m%d")
    sessions = pd.DatetimeIndex(
        pd.to_datetime(
            frame.loc[
                frame["is_open"].eq("1")
                & frame["cal_date"].between(start_text, end_text),
                "cal_date",
            ],
            format="%Y%m%d",
        )
    )
    if sessions.empty or sessions[0] != start or sessions[-1] != end:
        raise DataError("price range endpoints must both be open Tushare SSE sessions")
    return sessions


def load_bundle_calendar(bundle_path: str | Path) -> SessionCalendar:
    root = Path(bundle_path)
    manifest = load_assets_manifest(root / "assets.pk")
    sessions = ints_to_dates(load_trade_dates(root / "trade_dates.npy"))
    if len(sessions) != int(manifest["session_count"]):
        raise DataError("Bundle calendar session count does not match assets.pk")
    return SessionCalendar(sessions)
