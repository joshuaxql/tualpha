"""Orders, transactions, lots, and portfolio state."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from itertools import count

import pandas as pd

from .assets import Asset

_EPSILON = 1e-9
_ORDER_IDS = count(1)
_TRANSACTION_IDS = count(1)


class OrderStatus(StrEnum):
    OPEN = "open"
    FILLED = "filled"
    PARTIALLY_FILLED = "partially_filled"
    REJECTED = "rejected"
    CANCELED = "canceled"


class RejectReason(StrEnum):
    LIMIT_UP = "limit_up"
    LIMIT_DOWN = "limit_down"
    SUSPENDED = "suspended"
    NO_MARKET_DATA = "no_market_data"
    ZERO_VOLUME = "zero_volume"
    INVALID_LOT = "invalid_lot"
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
    handling_fee: float = 0.0
    transfer_fee: float = 0.0
    included_handling_fee: float = 0.0

    @property
    def total(self) -> float:
        return self.commission + self.stamp_tax + self.handling_fee + self.transfer_fee

    def __add__(self, other: FeeBreakdown) -> FeeBreakdown:
        return FeeBreakdown(
            commission=self.commission + other.commission,
            stamp_tax=self.stamp_tax + other.stamp_tax,
            handling_fee=self.handling_fee + other.handling_fee,
            transfer_fee=self.transfer_fee + other.transfer_fee,
            included_handling_fee=(
                self.included_handling_fee + other.included_handling_fee
            ),
        )


@dataclass(slots=True)
class Order:
    asset: Asset
    amount: float
    created_session: pd.Timestamp
    eligible_session: pd.Timestamp
    id: int = field(default_factory=lambda: next(_ORDER_IDS))
    status: OrderStatus = OrderStatus.OPEN
    filled: float = 0.0
    average_price: float = 0.0
    reject_reason: RejectReason | None = None
    message: str = ""

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

    def reject(self, reason: RejectReason, message: str) -> None:
        self.status = OrderStatus.REJECTED
        self.reject_reason = reason
        self.message = message

    def cancel(self, reason: RejectReason, message: str) -> None:
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


@dataclass(slots=True)
class PositionLot:
    quantity: float
    unit_cost: float
    acquired_session: pd.Timestamp
    settle_session: pd.Timestamp

    def apply_adjustment(self, ratio: float) -> None:
        self.quantity *= ratio
        self.unit_cost /= ratio


@dataclass(slots=True)
class Position:
    asset: Asset
    lots: list[PositionLot] = field(default_factory=list)
    last_sale_price: float = 0.0
    _cached_amount: float | None = field(default=None, init=False, repr=False)
    _cached_total_cost: float | None = field(default=None, init=False, repr=False)

    def _invalidate_totals(self) -> None:
        self._cached_amount = None
        self._cached_total_cost = None

    @property
    def amount(self) -> float:
        if self._cached_amount is None:
            self._cached_amount = sum(lot.quantity for lot in self.lots)
        return self._cached_amount

    @property
    def total_cost(self) -> float:
        if self._cached_total_cost is None:
            self._cached_total_cost = sum(
                lot.quantity * lot.unit_cost for lot in self.lots
            )
        return self._cached_total_cost

    @property
    def cost_basis(self) -> float:
        amount = self.amount
        return self.total_cost / amount if amount > _EPSILON else 0.0

    def sellable_amount(self, session: pd.Timestamp) -> float:
        return sum(lot.quantity for lot in self.lots if lot.settle_session <= session)

    def add_lot(
        self,
        quantity: float,
        unit_cost: float,
        acquired_session: pd.Timestamp,
        settle_session: pd.Timestamp,
    ) -> None:
        self.lots.append(
            PositionLot(
                quantity=quantity,
                unit_cost=unit_cost,
                acquired_session=acquired_session,
                settle_session=settle_session,
            )
        )
        self._invalidate_totals()

    def apply_adjustment(self, ratio: float) -> None:
        if ratio <= 0:
            raise ValueError("adjustment ratio must be positive")
        if abs(ratio - 1.0) <= _EPSILON:
            return
        for lot in self.lots:
            lot.apply_adjustment(ratio)
        self._invalidate_totals()

    def consume(
        self, quantity: float, session: pd.Timestamp
    ) -> list[tuple[float, float, pd.Timestamp]]:
        """Consume settled lots FIFO and return quantity/cost/acquisition tuples."""

        if quantity <= 0:
            raise ValueError("sell quantity must be positive")
        remaining = quantity
        consumed: list[tuple[float, float, pd.Timestamp]] = []
        new_lots: list[PositionLot] = []
        for lot in self.lots:
            if remaining > _EPSILON and lot.settle_session <= session:
                take = min(lot.quantity, remaining)
                consumed.append((take, lot.unit_cost, lot.acquired_session))
                lot.quantity -= take
                remaining -= take
            if lot.quantity > _EPSILON:
                new_lots.append(lot)
        if remaining > 1e-6:
            raise ValueError("sell quantity exceeds settled position")
        self.lots = new_lots
        self._invalidate_totals()
        return consumed


@dataclass(frozen=True, slots=True)
class ClosedTrade:
    asset: Asset
    entry_session: pd.Timestamp
    exit_session: pd.Timestamp
    quantity: float
    entry_price: float
    exit_price: float
    pnl: float
    fees: float

    @property
    def holding_days(self) -> int:
        return max(0, (self.exit_session - self.entry_session).days)


class Portfolio:
    """Long-only cash portfolio with lot-level settlement state."""

    def __init__(self, capital_base: float) -> None:
        self.starting_cash = float(capital_base)
        self.cash = float(capital_base)
        self.positions: dict[Asset, Position] = {}
        self.positions_value = 0.0
        self.portfolio_value = float(capital_base)
        self.pnl = 0.0
        self.returns = 0.0

    def position(self, asset: Asset) -> Position | None:
        return self.positions.get(asset)

    def amount(self, asset: Asset) -> float:
        position = self.position(asset)
        return position.amount if position is not None else 0.0

    def sellable_amount(self, asset: Asset, session: pd.Timestamp) -> float:
        position = self.position(asset)
        return position.sellable_amount(session) if position is not None else 0.0

    def apply_adjustment(self, asset: Asset, ratio: float) -> None:
        position = self.position(asset)
        if position is not None:
            position.apply_adjustment(ratio)

    def mark_to_market(self, prices: dict[Asset, float]) -> None:
        positions_value = 0.0
        for asset, position in self.positions.items():
            valuation_price = prices.get(asset, position.last_sale_price)
            if valuation_price > 0:
                position.last_sale_price = valuation_price
            positions_value += position.amount * max(0.0, valuation_price)
        self.positions_value = positions_value
        self.portfolio_value = self.cash + positions_value
        self.pnl = self.portfolio_value - self.starting_cash
        self.returns = self.pnl / self.starting_cash

    def remove_empty_positions(self) -> None:
        self.positions = {
            asset: position
            for asset, position in self.positions.items()
            if position.amount > _EPSILON
        }
