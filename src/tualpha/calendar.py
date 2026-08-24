"""China trading sessions attached to the current Zipline bundle."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
from zipline.utils.calendar_utils import get_calendar

from .bundle import (
    BUNDLE_NAME,
    acquire_bundle_read_lock,
    latest_bundle_path,
    release_bundle_read_lock,
)
from .config import normalize_session
from .exceptions import DataError


class ChinaTradingCalendar:
    """Daily XSHG sessions clipped to the latest bundle's date range."""

    def __init__(self, bundle_root: str | Path, bundle_name: str = BUNDLE_NAME) -> None:
        lock_key, _ = acquire_bundle_read_lock(bundle_root, bundle_name)
        try:
            self.bundle_path = latest_bundle_path(bundle_root, bundle_name)
            manifest_path = self.bundle_path / "manifest.json"
            if not manifest_path.is_file():
                raise DataError(f"bundle manifest does not exist: {manifest_path}")
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.bundle_generation = str(manifest["generated_at"])
            start = normalize_session(manifest["start_session"])
            end = normalize_session(manifest["end_session"])
            exchange_calendar = get_calendar("XSHG")
            self._sessions = exchange_calendar.sessions_in_range(start, end)
            if self._sessions.empty:
                raise DataError(f"bundle contains no trading sessions: {manifest_path}")
            self._positions = {
                session: position for position, session in enumerate(self._sessions)
            }
        finally:
            release_bundle_read_lock(lock_key)

    @property
    def first_session(self) -> pd.Timestamp:
        return self._sessions[0]

    @property
    def last_session(self) -> pd.Timestamp:
        return self._sessions[-1]

    @property
    def sessions(self) -> pd.DatetimeIndex:
        return self._sessions

    def is_session(self, value: str | pd.Timestamp) -> bool:
        return normalize_session(value) in self._positions

    def sessions_in_range(
        self, start: str | pd.Timestamp, end: str | pd.Timestamp
    ) -> pd.DatetimeIndex:
        start_date = normalize_session(start)
        end_date = normalize_session(end)
        return self._sessions[
            (self._sessions >= start_date) & (self._sessions <= end_date)
        ]

    def previous_session(self, value: str | pd.Timestamp) -> pd.Timestamp:
        date = normalize_session(value)
        index = self._sessions.searchsorted(date, side="left") - 1
        if index < 0:
            raise DataError(f"no bundled trading session before {date.date()}")
        return self._sessions[index]

    def next_session(self, value: str | pd.Timestamp) -> pd.Timestamp:
        date = normalize_session(value)
        index = self._sessions.searchsorted(date, side="right")
        if index >= len(self._sessions):
            raise DataError(f"no bundled trading session after {date.date()}")
        return self._sessions[index]

    def session_offset(self, value: str | pd.Timestamp, offset: int) -> pd.Timestamp:
        date = normalize_session(value)
        position = self._positions.get(date)
        if position is None:
            raise DataError(f"not a bundled trading session: {date.date()}")
        target = position + offset
        if target < 0 or target >= len(self._sessions):
            raise DataError(f"calendar offset {offset} is outside bundled data")
        return self._sessions[target]

    def window(
        self, end: str | pd.Timestamp, count: int, include_end: bool = True
    ) -> pd.DatetimeIndex:
        if count <= 0:
            raise ValueError("count must be positive")
        date = normalize_session(end)
        right = self._sessions.searchsorted(
            date, side="right" if include_end else "left"
        )
        left = max(0, right - count)
        return self._sessions[left:right]
