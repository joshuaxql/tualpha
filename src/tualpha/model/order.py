"""Order and transaction domain models."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from itertools import count

import pandas as pd

from .asset import Asset

_ORDER_IDS = count(1)
_TRANSACTION_IDS = count(1)


class OrderStatus(StrEnum):
    OPEN = "open"
    FILLED = "filled"
    PARTIALLY_FILLED = "partially_filled"
    REJECTED = "rejected"
    CANCELED = "canceled"


class OrderSizing(StrEnum):
    QUANTITY = "quantity"
    VALUE = "value"
    PERCENT = "percent"
    TARGET_QUANTITY = "target_quantity"
    TARGET_VALUE = "target_value"
    TARGET_PERCENT = "target_percent"


class RejectReason(StrEnum):
    LIMIT_UP = "limit_up"
    LIMIT_DOWN = "limit_down"
    SUSPENDED = "suspended"
    NO_MARKET_DATA = "no_market_data"
    ZERO_VOLUME = "zero_volume"
    INVALID_LOT = "invalid_lot"
    BELOW_MINIMUM_ORDER = "below_minimum_order"
    T_PLUS_ONE = "t_plus_one"
    INSUFFICIENT_CASH = "insufficient_cash"
    INSUFFICIENT_POSITION = "insufficient_position"
    ASSET_NOT_ALIVE = "asset_not_alive"
    END_OF_BACKTEST = "end_of_backtest"
    ZERO_AMOUNT = "zero_amount"


@dataclass(slots=True)
class FeeBreakdown:
    commission: float = 0.0
    stamp_tax: float = 0.0
    transfer_fee: float = 0.0

    @property
    def total(self) -> float:
        return self.commission + self.stamp_tax + self.transfer_fee

    def __add__(self, other: FeeBreakdown) -> FeeBreakdown:
        return FeeBreakdown(
            commission=self.commission + other.commission,
            stamp_tax=self.stamp_tax + other.stamp_tax,
            transfer_fee=self.transfer_fee + other.transfer_fee,
        )


@dataclass(slots=True)
class Order:
    asset: Asset
    amount: float
    created_session: pd.Timestamp
    eligible_session: pd.Timestamp
    position_limit: int | None = None
    cash_adaptive: bool = False
    sizing: OrderSizing = OrderSizing.QUANTITY
    requested: float | None = None
    is_batch: bool = False
    is_target: bool = False
    id: int = field(default_factory=lambda: next(_ORDER_IDS))
    status: OrderStatus = OrderStatus.OPEN
    filled: float = 0.0
    average_price: float = 0.0
    reject_reason: RejectReason | None = None
    message: str = ""

    def __post_init__(self) -> None:
        if self.requested is None:
            self.requested = float(self.amount)

    @property
    def remaining(self) -> float:
        return self.amount - self.filled

    @property
    def is_buy(self) -> bool:
        return self.amount > 0

    @property
    def is_sell(self) -> bool:
        return self.amount < 0

    @property
    def open(self) -> bool:
        return self.status is OrderStatus.OPEN

    def resolve_amount(self, amount: float) -> None:
        self.amount = float(amount)

    def reject(self, reason: RejectReason, message: str) -> None:
        self.status = OrderStatus.REJECTED
        self.reject_reason = reason
        self.message = message

    def cancel(self, reason: RejectReason | None, message: str) -> None:
        self.status = OrderStatus.CANCELED
        self.reject_reason = reason
        self.message = message


@dataclass(frozen=True, slots=True)
class Transaction:
    order_id: int
    asset: Asset
    amount: float
    price: float
    session: pd.Timestamp
    fees: FeeBreakdown
    id: int = field(default_factory=lambda: next(_TRANSACTION_IDS))

    @property
    def gross_value(self) -> float:
        return abs(self.amount * self.price)

    @property
    def cash_flow(self) -> float:
        gross = self.amount * self.price
        return -gross - self.fees.total
