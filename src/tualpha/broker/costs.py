"""Date-aware A-share and ETF transaction costs."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from decimal import ROUND_HALF_UP, Decimal

import pandas as pd

from ..config import normalize_session
from ..model.asset import Asset, AssetType
from ..model.order import FeeBreakdown


def _money(value: float) -> float:
    return float(
        Decimal(str(max(0.0, value))).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    )


@dataclass(frozen=True, slots=True)
class RateSchedule:
    """Piecewise-constant rates keyed by inclusive effective dates."""

    entries: tuple[tuple[pd.Timestamp, float], ...]

    @classmethod
    def from_pairs(
        cls, pairs: Iterable[tuple[str | pd.Timestamp, float]]
    ) -> RateSchedule:
        entries = tuple(
            sorted(
                ((normalize_session(date), float(rate)) for date, rate in pairs),
                key=lambda item: item[0],
            )
        )
        if not entries:
            raise ValueError("rate schedule must not be empty")
        if any(rate < 0 for _, rate in entries):
            raise ValueError("rates must be non-negative")
        return cls(entries)

    def rate_on(self, session: str | pd.Timestamp) -> float:
        date = normalize_session(session)
        rate = self.entries[0][1]
        for effective, candidate in self.entries:
            if effective > date:
                break
            rate = candidate
        return rate


_DEFAULT_STAMP_TAX = RateSchedule.from_pairs(
    (("1900-01-01", 0.001), ("2023-08-28", 0.0005))
)
_DEFAULT_TRANSFER = RateSchedule.from_pairs(
    (("1900-01-01", 0.00002), ("2022-04-29", 0.00001))
)


@dataclass(slots=True)
class ChinaFeeModel:
    """Broker-realistic default with explicit, non-duplicated fee components.

    Commission is treated as an all-in brokerage charge that already contains
    exchange handling fees; handling fees are never charged separately.
    """

    stock_commission_rate: float = 0.0003
    etf_commission_rate: float = 0.0003
    stock_min_commission: float = 5.0
    etf_min_commission: float = 5.0
    stamp_tax: RateSchedule = field(default_factory=lambda: _DEFAULT_STAMP_TAX)
    stock_transfer: RateSchedule = field(default_factory=lambda: _DEFAULT_TRANSFER)

    def __post_init__(self) -> None:
        values = (
            self.stock_commission_rate,
            self.etf_commission_rate,
            self.stock_min_commission,
            self.etf_min_commission,
        )
        if any(value < 0 for value in values):
            raise ValueError("commission rates and minimums must be non-negative")

    def calculate(
        self,
        asset: Asset,
        gross_value: float,
        *,
        is_sell: bool,
        session: str | pd.Timestamp,
    ) -> FeeBreakdown:
        value = abs(float(gross_value))
        if value == 0:
            return FeeBreakdown()

        if asset.asset_type is AssetType.STOCK:
            commission_rate = self.stock_commission_rate
            minimum = self.stock_min_commission
            stamp_tax = value * self.stamp_tax.rate_on(session) if is_sell else 0.0
            transfer_fee = value * self.stock_transfer.rate_on(session)
        else:
            commission_rate = self.etf_commission_rate
            minimum = self.etf_min_commission
            stamp_tax = 0.0
            transfer_fee = 0.0

        commission = (
            max(value * commission_rate, minimum) if commission_rate or minimum else 0.0
        )
        return FeeBreakdown(
            commission=_money(commission),
            stamp_tax=_money(stamp_tax),
            transfer_fee=_money(transfer_fee),
        )
