"""Long-only portfolio, positions, and settled lots."""

from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

from .asset import Asset

_EPSILON = 1e-9


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
        unsettled = 0.0
        for lot in reversed(self.lots):
            if lot.settle_session <= session:
                break
            unsettled += lot.quantity
        return self.amount - unsettled

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
