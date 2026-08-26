"""China trading sessions stored in ``trade_dates.npy``."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from ._calendar_store import SessionCalendar, load_bundle_calendar
from ._hdf5_store import load_assets_manifest
from .bundle import (
    BUNDLE_NAME,
    acquire_bundle_read_lock,
    latest_bundle_path,
    release_bundle_read_lock,
)
from .exceptions import DataError


class ChinaTradingCalendar:
    """Daily XSHG sessions frozen from Tushare ``trade_cal`` data."""

    def __init__(self, bundle_root: str | Path, bundle_name: str = BUNDLE_NAME) -> None:
        lock_key, _ = acquire_bundle_read_lock(bundle_root, bundle_name)
        try:
            self.bundle_path = latest_bundle_path(bundle_root, bundle_name)
            manifest = load_assets_manifest(self.bundle_path / "assets.pk")
            self.bundle_generation = str(manifest["generation"])
            self._calendar: SessionCalendar = load_bundle_calendar(self.bundle_path)
            self._sessions = self._calendar.sessions
        finally:
            release_bundle_read_lock(lock_key)

    @property
    def first_session(self) -> pd.Timestamp:
        return self._calendar.first_session

    @property
    def last_session(self) -> pd.Timestamp:
        return self._calendar.last_session

    @property
    def sessions(self) -> pd.DatetimeIndex:
        return self._sessions

    def is_session(self, value: str | pd.Timestamp) -> bool:
        return self._calendar.is_session(value)

    def sessions_in_range(
        self, start: str | pd.Timestamp, end: str | pd.Timestamp
    ) -> pd.DatetimeIndex:
        return self._calendar.sessions_in_range(start, end)

    def previous_session(self, value: str | pd.Timestamp) -> pd.Timestamp:
        try:
            return self._calendar.previous_session(value)
        except IndexError as exc:
            raise DataError(str(exc)) from exc

    def next_session(self, value: str | pd.Timestamp) -> pd.Timestamp:
        try:
            return self._calendar.next_session(value)
        except IndexError as exc:
            raise DataError(str(exc)) from exc

    def session_offset(self, value: str | pd.Timestamp, offset: int) -> pd.Timestamp:
        try:
            return self._calendar.session_offset(value, offset)
        except IndexError as exc:
            raise DataError(str(exc)) from exc

    def window(
        self, end: str | pd.Timestamp, count: int, include_end: bool = True
    ) -> pd.DatetimeIndex:
        return self._calendar.window(end, count, include_end)
