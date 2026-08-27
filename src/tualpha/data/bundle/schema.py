"""In-memory compound dtypes shared by Bundle readers and writers."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

import numpy as np
import pandas as pd

from ...foundation.exceptions import DataError
from ..tushare_fields import FINANCIAL_FIELDS


def dates_to_int(values: Sequence[object] | pd.DatetimeIndex) -> np.ndarray:
    dates = (
        values
        if isinstance(values, pd.DatetimeIndex)
        else pd.DatetimeIndex(pd.to_datetime(list(values)))
    )
    if dates.tz is not None:
        dates = dates.tz_convert("Asia/Shanghai").tz_localize(None)
    return dates.strftime("%Y%m%d").astype(np.int32).to_numpy(dtype="<i4")


CODE_BYTES = 16
CATEGORY_BYTES = 128

DAILY_DTYPE = np.dtype(
    [
        ("trade_date", "<i4"),
        ("open", "<f8"),
        ("high", "<f8"),
        ("low", "<f8"),
        ("close", "<f8"),
        ("pre_close", "<f8"),
        ("volume", "<f8"),
        ("turnover", "<f8"),
    ]
)


@dataclass(frozen=True, slots=True)
class DailyRole:
    role: str
    source_names: tuple[str, ...]
    numeric_fields: tuple[str, ...] = ()
    categorical_fields: tuple[str, ...] = ()
    flag_fields: tuple[str, ...] = ()


DAILY_ROLES = (
    DailyRole("adj_factor", ("adj_factor", "fund_adj"), ("adj_factor",)),
    DailyRole(
        "daily_basic",
        ("daily_basic",),
        (
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
        ),
    ),
    DailyRole("stk_limit", ("stk_limit",), ("up_limit", "down_limit")),
    DailyRole(
        "industry",
        ("industry",),
        (),
        ("l1_code", "l1_name", "l2_code", "l2_name", "l3_code", "l3_name"),
    ),
    DailyRole(
        "stock_st",
        ("stock_st",),
        (),
        ("name", "type", "type_name"),
        ("is_st",),
    ),
    DailyRole(
        "moneyflow",
        ("moneyflow",),
        (
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
        ),
    ),
    DailyRole("suspend_d", ("suspend_d",), (), (), ("suspended",)),
)


def role_update_dtype(role: DailyRole) -> np.dtype:
    return np.dtype(
        [
            ("ts_code", f"S{CODE_BYTES}"),
            ("trade_date", "<i4"),
            ("source_order", "<u8"),
            *[(field, "<f8") for field in role.numeric_fields],
            *[(field, f"S{CATEGORY_BYTES}") for field in role.categorical_fields],
            *[(field, "u1") for field in role.flag_fields],
        ]
    )


def role_dtype(role: DailyRole) -> np.dtype:
    return np.dtype(
        [
            ("trade_date", "<i4"),
            *[(field, "<f8") for field in role.numeric_fields],
            *[(field, "<i4") for field in role.categorical_fields],
            *[(field, "u1") for field in role.flag_fields],
        ]
    )


FINANCIAL_KEYS = {
    "ts_code",
    "ann_date",
    "f_ann_date",
    "end_date",
    "report_type",
    "comp_type",
    "end_type",
    "update_flag",
}


def finance_fields(table: str) -> list[str]:
    return [field for field in FINANCIAL_FIELDS[table] if field not in FINANCIAL_KEYS]


def finance_update_dtype(table: str) -> np.dtype:
    return np.dtype(
        [
            ("ts_code", f"S{CODE_BYTES}"),
            ("ann_date", "<i4"),
            ("f_ann_date", "<i4"),
            ("effective_ann_date", "<i4"),
            ("end_date", "<i4"),
            ("report_type", "S16"),
            ("comp_type", "S16"),
            ("end_type", "S16"),
            ("update_flag", "S16"),
            ("source_order", "<u8"),
            *[(field, "<f8") for field in finance_fields(table)],
        ]
    )


def empty_daily_array(sessions: pd.DatetimeIndex, dtype: np.dtype) -> np.ndarray:
    values = np.empty(len(sessions), dtype=dtype)
    values["trade_date"] = dates_to_int(sessions)
    for field in dtype.names or ():
        if field == "trade_date":
            continue
        kind = dtype[field].kind
        if kind == "f":
            values[field] = np.nan
        elif kind == "i":
            values[field] = -1
        else:
            values[field] = 0
    return values


def place_rows(
    target: np.ndarray,
    rows: np.ndarray,
    value_fields: Sequence[str],
) -> None:
    dates = target["trade_date"]
    positions = np.searchsorted(dates, rows["trade_date"])
    valid = positions < len(dates)
    valid[valid] &= dates[positions[valid]] == rows["trade_date"][valid]
    if not valid.all():
        bad = rows["trade_date"][~valid][:5].tolist()
        raise DataError(f"daily rows fall outside the asset calendar: {bad}")
    for field in value_fields:
        target[field][positions] = rows[field]
