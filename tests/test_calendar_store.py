from __future__ import annotations

import pandas as pd
import pytest

from tualpha.data.bundle.calendar_store import (
    SessionCalendar,
    normalize_trade_calendar,
    sessions_from_trade_calendar,
)
from tualpha.exceptions import DataError


def _calendar_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "exchange": ["SSE", "SSE", "SSE"],
            "cal_date": ["20240101", "20240102", "20240103"],
            "is_open": ["0", "1", "1"],
            "pretrade_date": ["20231229", "20231229", "20240102"],
        }
    )


def test_trade_calendar_normalization_sorts_and_deduplicates() -> None:
    source = _calendar_frame()
    source = pd.concat(
        [source.iloc[::-1], source.iloc[[1]], source.assign(exchange="SZSE")],
        ignore_index=True,
    )

    normalized = normalize_trade_calendar(source)

    assert normalized["cal_date"].tolist() == ["20240101", "20240102", "20240103"]
    assert normalized["exchange"].unique().tolist() == ["SSE"]
    sessions = sessions_from_trade_calendar(normalized, "2024-01-02", "2024-01-03")
    assert list(sessions) == [pd.Timestamp("2024-01-02"), pd.Timestamp("2024-01-03")]


def test_trade_calendar_rejects_conflicts_gaps_and_bad_pretrade_dates() -> None:
    source = _calendar_frame()
    conflict = pd.concat(
        [
            source,
            pd.DataFrame(
                [["SSE", "20240102", "0", "20231229"]],
                columns=source.columns,
            ),
        ],
        ignore_index=True,
    )
    with pytest.raises(DataError, match="conflicting is_open"):
        normalize_trade_calendar(conflict)

    with pytest.raises(DataError, match="missing natural dates"):
        normalize_trade_calendar(source[source["cal_date"] != "20240102"])

    bad_previous = source.copy()
    bad_previous.loc[bad_previous["cal_date"] == "20240103", "pretrade_date"] = (
        "20231229"
    )
    with pytest.raises(DataError, match="pretrade_date mismatch"):
        normalize_trade_calendar(bad_previous)


def test_session_calendar_ordinals_and_ordering() -> None:
    calendar = SessionCalendar(pd.DatetimeIndex(["2024-01-02", "2024-01-03"]))

    assert calendar.sessions.equals(pd.DatetimeIndex(["2024-01-02", "2024-01-03"]))
    assert calendar.next_session("2024-01-02") == pd.Timestamp("2024-01-03")
    assert calendar.previous_session("2024-01-03") == pd.Timestamp("2024-01-02")

    with pytest.raises(DataError, match="strictly increasing"):
        SessionCalendar(pd.DatetimeIndex(["2024-01-03", "2024-01-02"]))
