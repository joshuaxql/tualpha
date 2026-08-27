"""Daily simulated broker for stocks and ETFs."""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Iterable

import pandas as pd

from ..data.portal import TushareDataPortal
from ..data.trading_calendar import ChinaTradingCalendar
from ..foundation.exceptions import DataError
from ..model.asset import Asset
from ..model.order import (
    FeeBreakdown,
    Order,
    OrderSizing,
    OrderStatus,
    RejectReason,
    Transaction,
)
from ..model.portfolio import ClosedTrade, Portfolio, Position
from .costs import ChinaFeeModel
from .market_rules import ChinaMarketRules
from .matcher import DailyBarMatcher

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
        self._fee_model = fee_model
        self.market_rules = market_rules
        self.execution_field = execution_field
        self.matcher = DailyBarMatcher(
            portfolio,
            fee_model,
            market_rules,
            execution_field,
        )
        self.orders: list[Order] = []
        self._open_orders: dict[int, Order] = {}
        self.transactions: list[Transaction] = []
        self.closed_trades: list[ClosedTrade] = []
        self._fees_by_session: dict[pd.Timestamp, FeeBreakdown] = defaultdict(
            FeeBreakdown
        )

    @property
    def fee_model(self) -> ChinaFeeModel:
        return self._fee_model

    @fee_model.setter
    def fee_model(self, model: ChinaFeeModel) -> None:
        self._fee_model = model
        self.matcher.fee_model = model

    def _append_order(
        self,
        asset: Asset,
        requested: float,
        created_session: pd.Timestamp,
        eligible_session: pd.Timestamp,
        *,
        sizing: OrderSizing = OrderSizing.QUANTITY,
        position_limit: int | None = None,
        cash_adaptive: bool = False,
        is_batch: bool = False,
        is_target: bool = False,
    ) -> Order:
        numeric_request = float(requested)
        initial_amount = (
            numeric_request
            if sizing in {OrderSizing.QUANTITY, OrderSizing.TARGET_QUANTITY}
            else 0.0
        )
        order = Order(
            asset=asset,
            amount=initial_amount,
            created_session=created_session,
            eligible_session=eligible_session,
            position_limit=position_limit,
            cash_adaptive=cash_adaptive,
            sizing=sizing,
            requested=numeric_request,
            is_batch=is_batch,
            is_target=is_target,
        )
        if not math.isfinite(numeric_request):
            order.reject(RejectReason.INVALID_LOT, "委托参数必须为有限数值")
        elif abs(numeric_request) <= _EPSILON and not (
            is_target
            and sizing in {OrderSizing.TARGET_VALUE, OrderSizing.TARGET_PERCENT}
        ):
            order.reject(RejectReason.ZERO_AMOUNT, "委托参数为零")
        self.orders.append(order)
        if order.open:
            self._open_orders[order.id] = order
        return order

    def submit(
        self,
        asset: Asset,
        requested: float,
        created_session: pd.Timestamp,
        eligible_session: pd.Timestamp,
        *,
        sizing: OrderSizing = OrderSizing.QUANTITY,
        position_limit: int | None = None,
        cash_adaptive: bool = False,
        is_batch: bool = False,
        is_target: bool = False,
    ) -> Order:
        return self._append_order(
            asset,
            requested,
            created_session,
            eligible_session,
            sizing=sizing,
            position_limit=position_limit,
            cash_adaptive=cash_adaptive,
            is_batch=is_batch,
            is_target=is_target,
        )

    def submit_many(
        self,
        orders: Iterable[tuple[Asset, float, OrderSizing, bool]],
        created_session: pd.Timestamp,
        eligible_session: pd.Timestamp,
        position_limit: int | None = None,
        cash_adaptive: bool = False,
    ) -> list[Order]:
        """Append a mapping-ordered batch without repeating session resolution."""

        append = self._append_order
        return [
            append(
                asset,
                requested,
                created_session,
                eligible_session,
                sizing=sizing,
                position_limit=position_limit,
                cash_adaptive=cash_adaptive,
                is_batch=True,
                is_target=is_target,
            )
            for asset, requested, sizing, is_target in orders
        ]

    def cancel(self, order: Order) -> None:
        if order.open:
            order.status = OrderStatus.CANCELED
            order.message = "用户撤单"
            self._open_orders.pop(order.id, None)

    def get_open_orders(self, asset: Asset | None = None) -> list[Order]:
        return [
            order
            for order in self._open_orders.values()
            if order.open and (asset is None or order.asset == asset)
        ]

    def apply_corporate_actions(
        self, previous_session: pd.Timestamp | None, session: pd.Timestamp
    ) -> None:
        """Adjust existing lots and preserve pending full-liquidation intent."""

        if previous_session is None:
            return
        open_sells: dict[Asset, list[Order]] = defaultdict(list)
        for order in self._open_orders.values():
            if order.open and order.is_sell:
                open_sells[order.asset].append(order)
        for asset, position in tuple(self.portfolio.positions.items()):
            previous_factor = self.data_portal.factor(asset, previous_session)
            current_factor = self.data_portal.factor(asset, session)
            if previous_factor <= 0 or current_factor <= 0:
                continue
            ratio = current_factor / previous_factor
            if abs(ratio - 1.0) <= _EPSILON:
                continue
            previous_amount = position.amount
            liquidation_orders = [
                order
                for order in open_sells.get(asset, ())
                if abs(abs(order.remaining) - previous_amount) <= _EPSILON
            ]
            self.portfolio.apply_adjustment(asset, ratio)
            adjusted_amount = position.amount
            for order in liquidation_orders:
                order.amount = order.filled - adjusted_amount
                order.requested = order.amount

    def _pre_match_equity(
        self,
        session: pd.Timestamp,
        bars: dict[Asset, object],
    ) -> float:
        value = self.portfolio.cash
        for asset, position in self.portfolio.positions.items():
            if not asset.is_alive_on(session):
                continue
            price = position.last_sale_price
            bar = bars.get(asset)
            if bar is not None:
                candidate = bar.value(self.execution_field)
                if pd.notna(candidate) and candidate > 0:
                    price = float(candidate)
            value += position.amount * max(0.0, price)
        return value

    def process_orders(self, session: pd.Timestamp) -> list[Transaction]:
        fills: list[Transaction] = []
        eligible = [
            order
            for order in self._open_orders.values()
            if order.open and order.eligible_session <= session
        ]
        if not eligible:
            return fills
        requested_assets = [order.asset for order in eligible]
        held_assets = list(self.portfolio.positions)
        bars = self.data_portal.execution_bars(
            list(dict.fromkeys([*held_assets, *requested_assets])),
            session,
            self.execution_field,
        )
        pre_match_equity = self._pre_match_equity(session, bars)
        active_position_count = sum(
            1
            for asset, position in self.portfolio.positions.items()
            if position.amount > _EPSILON
            and position.last_sale_price > 0
            and asset.is_alive_on(session)
        )
        settle_session: pd.Timestamp | None = None
        for order in eligible:
            asset = order.asset
            if not asset.is_alive_on(session):
                order.reject(
                    RejectReason.ASSET_NOT_ALIVE, "证券在成交日尚未上市或已经退市"
                )
                continue
            position = self.portfolio.position(asset)
            opens_new_position = position is None or position.amount <= _EPSILON
            matched = self.matcher.match(
                order,
                bars.get(asset),
                session,
                pre_match_equity,
                active_position_count,
            )
            if matched is None:
                continue
            transaction = Transaction(
                order_id=order.id,
                asset=asset,
                amount=matched.amount,
                price=matched.price,
                session=session,
                fees=matched.fees,
            )
            if transaction.amount > 0:
                if settle_session is None:
                    try:
                        settle_session = self.calendar.next_session(session)
                    except DataError:
                        settle_session = session + pd.Timedelta(days=1)
                self._apply_buy(transaction, settle_session)
                if opens_new_position:
                    active_position_count += 1
            else:
                self._apply_sell(transaction)
                if asset not in self.portfolio.positions:
                    active_position_count = max(0, active_position_count - 1)

            self.transactions.append(transaction)
            self._fees_by_session[session] = (
                self._fees_by_session[session] + transaction.fees
            )
            order.filled += transaction.amount
            order.average_price = transaction.price
            order.status = (
                OrderStatus.FILLED
                if abs(abs(transaction.amount) - abs(order.amount)) <= _EPSILON
                else OrderStatus.PARTIALLY_FILLED
            )
            if order.status is OrderStatus.PARTIALLY_FILLED:
                order.message = "受限定金额或可用现金约束，订单按有效交易单位部分成交"
            fills.append(transaction)
        for order in eligible:
            if not order.open:
                self._open_orders.pop(order.id, None)
        return fills

    def _apply_buy(
        self, transaction: Transaction, settle_session: pd.Timestamp
    ) -> None:
        asset = transaction.asset
        quantity = transaction.amount
        self.portfolio.cash += transaction.cash_flow
        position = self.portfolio.positions.setdefault(asset, Position(asset=asset))
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
        if position.amount <= _EPSILON:
            self.portfolio.positions.pop(asset, None)

    def mark_to_market(self, session: pd.Timestamp) -> None:
        positions = list(self.portfolio.positions.items())
        active_assets = [
            asset
            for asset, _ in positions
            if asset.delist_date is None or session <= asset.delist_date
        ]
        closes = self.data_portal.values(
            active_assets,
            session,
            ["close"],
            adjusted=False,
        )["close"]
        close_by_asset = dict(zip(active_assets, closes, strict=True))
        prices = {}
        for asset, position in positions:
            if asset.delist_date is not None and session > asset.delist_date:
                prices[asset] = 0.0
                continue
            price = close_by_asset.get(asset, float("nan"))
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
        for order in tuple(self._open_orders.values()):
            if order.open:
                order.cancel(
                    RejectReason.END_OF_BACKTEST, "回测结束，订单未进入可成交交易日"
                )
        self._open_orders.clear()

    @property
    def total_fees(self) -> FeeBreakdown:
        total = FeeBreakdown()
        for fees in self._fees_by_session.values():
            total += fees
        return total

    def iter_positions(self) -> Iterable[tuple[Asset, Position]]:
        return self.portfolio.positions.items()
