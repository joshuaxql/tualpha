"""Daily simulated broker for stocks and ETFs."""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Iterable

import pandas as pd

from .assets import Asset
from .calendar import ChinaTradingCalendar
from .costs import ChinaFeeModel
from .data import TushareDataPortal
from .exceptions import DataError
from .models import (
    ClosedTrade,
    FeeBreakdown,
    Order,
    OrderStatus,
    Portfolio,
    Position,
    RejectReason,
    Transaction,
)
from .rules import ChinaMarketRules

_EPSILON = 1e-6


class SimulationBroker:
    """Long-only, one-attempt-per-session execution and accounting."""

    def __init__(
        self,
        portfolio: Portfolio,
        calendar: ChinaTradingCalendar,
        data_portal: TushareDataPortal,
        fee_model: ChinaFeeModel,
        market_rules: ChinaMarketRules,
        execution_field: str,
    ) -> None:
        if execution_field not in {"open", "close"}:
            raise ValueError("execution_field must be open or close")
        self.portfolio = portfolio
        self.calendar = calendar
        self.data_portal = data_portal
        self.fee_model = fee_model
        self.market_rules = market_rules
        self.execution_field = execution_field
        self.orders: list[Order] = []
        self.transactions: list[Transaction] = []
        self.closed_trades: list[ClosedTrade] = []
        self._fees_by_session: dict[pd.Timestamp, FeeBreakdown] = defaultdict(
            FeeBreakdown
        )

    def submit(
        self,
        asset: Asset,
        amount: float,
        created_session: pd.Timestamp,
        eligible_session: pd.Timestamp,
    ) -> Order:
        numeric_amount = float(amount)
        order = Order(
            asset=asset,
            amount=numeric_amount,
            created_session=created_session,
            eligible_session=eligible_session,
        )
        if not math.isfinite(numeric_amount):
            order.reject(RejectReason.INVALID_LOT, "委托数量必须为有限数值")
        elif abs(numeric_amount) <= _EPSILON:
            order.reject(RejectReason.ZERO_AMOUNT, "委托数量为零")
        self.orders.append(order)
        return order

    def cancel(self, order: Order) -> None:
        if order.open:
            order.status = OrderStatus.CANCELED
            order.message = "用户撤单"

    def get_open_orders(self, asset: Asset | None = None) -> list[Order]:
        return [
            order
            for order in self.orders
            if order.open and (asset is None or order.asset == asset)
        ]

    def apply_corporate_actions(
        self, previous_session: pd.Timestamp | None, session: pd.Timestamp
    ) -> None:
        """Apply Tushare's dividend-reinvestment factor to existing lots."""

        if previous_session is None:
            return
        for asset in tuple(self.portfolio.positions):
            previous_factor = self.data_portal.factor(asset, previous_session)
            current_factor = self.data_portal.factor(asset, session)
            if previous_factor > 0 and current_factor > 0:
                self.portfolio.apply_adjustment(asset, current_factor / previous_factor)

    def _reject(self, order: Order, failure: tuple[RejectReason, str] | None) -> bool:
        if failure is None:
            return False
        order.reject(*failure)
        return True

    def _affordable_buy_quantity(
        self, asset: Asset, requested: float, price: float, session: pd.Timestamp
    ) -> float:
        requested = self.market_rules.normalize_buy(asset, requested)
        if requested <= 0:
            return 0.0
        fees = self.fee_model.calculate(
            asset, requested * price, is_sell=False, session=session
        )
        if requested * price + fees.total <= self.portfolio.cash + _EPSILON:
            return requested

        rough = max(0.0, self.portfolio.cash / price)
        quantity = self.market_rules.normalize_buy(asset, min(requested, rough))
        # Fee minima and cent rounding can leave the rough estimate a few shares high.
        step = 1.0 if self.market_rules.allows_single_share_increment(asset) else 100.0
        while quantity > 0:
            fees = self.fee_model.calculate(
                asset, quantity * price, is_sell=False, session=session
            )
            if quantity * price + fees.total <= self.portfolio.cash + _EPSILON:
                return quantity
            quantity = self.market_rules.normalize_buy(asset, quantity - step)
        return 0.0

    def process_orders(self, session: pd.Timestamp) -> list[Transaction]:
        fills: list[Transaction] = []
        for order in self.orders:
            if not order.open or order.eligible_session > session:
                continue
            asset = order.asset
            if not asset.is_alive_on(session):
                order.reject(
                    RejectReason.ASSET_NOT_ALIVE, "证券在成交日尚未上市或已经退市"
                )
                continue
            bar = self.data_portal.raw_bar(asset, session)
            if self._reject(
                order,
                self.market_rules.validate_market(
                    asset,
                    bar,
                    is_buy=order.is_buy,
                    execution_field=self.execution_field,
                ),
            ):
                continue
            assert bar is not None  # validated above
            price = bar.value(self.execution_field)
            requested = abs(order.remaining)
            position = self.portfolio.position(asset)
            position_amount = position.amount if position is not None else 0.0

            if self._reject(
                order,
                self.market_rules.validate_quantity(
                    asset,
                    requested,
                    is_buy=order.is_buy,
                    position_amount=position_amount,
                ),
            ):
                continue

            if order.is_buy:
                quantity = self._affordable_buy_quantity(
                    asset, requested, price, session
                )
                if quantity <= 0:
                    order.reject(
                        RejectReason.INSUFFICIENT_CASH, "可用现金不足以完成最小交易单位"
                    )
                    continue
                fees = self.fee_model.calculate(
                    asset, quantity * price, is_sell=False, session=session
                )
                transaction = Transaction(
                    order_id=order.id,
                    asset=asset,
                    amount=quantity,
                    price=price,
                    session=session,
                    fees=fees,
                )
                self._apply_buy(transaction)
            else:
                quantity = requested
                if position is None or position_amount + _EPSILON < quantity:
                    order.reject(
                        RejectReason.INSUFFICIENT_POSITION, "持仓数量不足，禁止卖空"
                    )
                    continue
                sellable = position.sellable_amount(session)
                if sellable + _EPSILON < quantity:
                    order.reject(
                        RejectReason.T_PLUS_ONE, "可卖数量不足，证券尚未完成 T+1 交收"
                    )
                    continue
                fees = self.fee_model.calculate(
                    asset, quantity * price, is_sell=True, session=session
                )
                transaction = Transaction(
                    order_id=order.id,
                    asset=asset,
                    amount=-quantity,
                    price=price,
                    session=session,
                    fees=fees,
                )
                self._apply_sell(transaction)

            self.transactions.append(transaction)
            self._fees_by_session[session] = (
                self._fees_by_session[session] + transaction.fees
            )
            order.filled += transaction.amount
            order.average_price = transaction.price
            order.status = (
                OrderStatus.FILLED
                if abs(abs(transaction.amount) - requested) <= _EPSILON
                else OrderStatus.PARTIALLY_FILLED
            )
            if order.status is OrderStatus.PARTIALLY_FILLED:
                order.message = "受可用现金限制，订单按有效交易单位部分成交"
            fills.append(transaction)
        return fills

    def _apply_buy(self, transaction: Transaction) -> None:
        asset = transaction.asset
        quantity = transaction.amount
        self.portfolio.cash += transaction.cash_flow
        position = self.portfolio.positions.setdefault(asset, Position(asset=asset))
        try:
            settle_session = self.calendar.next_session(transaction.session)
        except DataError:
            # The final available data session can still hold an unsettled lot.
            settle_session = transaction.session + pd.Timedelta(days=1)
        unit_cost = (transaction.gross_value + transaction.fees.total) / quantity
        position.add_lot(
            quantity=quantity,
            unit_cost=unit_cost,
            acquired_session=transaction.session,
            settle_session=settle_session,
        )
        position.last_sale_price = transaction.price

    def _apply_sell(self, transaction: Transaction) -> None:
        asset = transaction.asset
        quantity = abs(transaction.amount)
        position = self.portfolio.positions[asset]
        consumed = position.consume(quantity, transaction.session)
        self.portfolio.cash += transaction.cash_flow
        for consumed_quantity, unit_cost, acquired_session in consumed:
            fraction = consumed_quantity / quantity
            allocated_exit_fees = transaction.fees.total * fraction
            pnl = (
                consumed_quantity * (transaction.price - unit_cost)
                - allocated_exit_fees
            )
            self.closed_trades.append(
                ClosedTrade(
                    asset=asset,
                    entry_session=acquired_session,
                    exit_session=transaction.session,
                    quantity=consumed_quantity,
                    entry_price=unit_cost,
                    exit_price=transaction.price,
                    pnl=pnl,
                    fees=allocated_exit_fees,
                )
            )
        position.last_sale_price = transaction.price
        self.portfolio.remove_empty_positions()

    def mark_to_market(self, session: pd.Timestamp) -> None:
        prices = {}
        for asset, position in self.portfolio.positions.items():
            if asset.delist_date is not None and session > asset.delist_date:
                prices[asset] = 0.0
                continue
            price = self.data_portal.value(asset, session, "close", adjusted=False)
            if pd.notna(price) and price > 0:
                prices[asset] = float(price)
            else:
                # Missing bars also represent ordinary suspensions, so a known
                # last price remains the safest point-in-time valuation.
                prices[asset] = position.last_sale_price
        self.portfolio.mark_to_market(prices)

    def fees_for_session(self, session: pd.Timestamp) -> FeeBreakdown:
        return self._fees_by_session.get(session, FeeBreakdown())

    def cancel_remaining(self) -> None:
        for order in self.orders:
            if order.open:
                order.cancel(
                    RejectReason.END_OF_BACKTEST, "回测结束，订单未进入可成交交易日"
                )

    @property
    def total_fees(self) -> FeeBreakdown:
        total = FeeBreakdown()
        for fees in self._fees_by_session.values():
            total += fees
        return total

    def iter_positions(self) -> Iterable[tuple[Asset, Position]]:
        return self.portfolio.positions.items()
