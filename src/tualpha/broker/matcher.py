"""D+1 daily-bar order sizing and matching."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from ..data.bar import DailyBar
from ..model.order import FeeBreakdown, Order, OrderSizing, RejectReason
from ..model.portfolio import Portfolio
from .costs import ChinaFeeModel
from .market_rules import ChinaMarketRules

_EPSILON = 1e-6


@dataclass(frozen=True, slots=True)
class MatchResult:
    amount: float
    price: float
    fees: FeeBreakdown


@dataclass(frozen=True, slots=True)
class _Intent:
    amount: float
    spending_limit: float | None
    adaptive: bool


class DailyBarMatcher:
    """Resolve an order intent with the selected D+1 bar endpoint."""

    def __init__(
        self,
        portfolio: Portfolio,
        fee_model: ChinaFeeModel,
        market_rules: ChinaMarketRules,
        execution_field: str,
    ) -> None:
        if execution_field not in {"open", "close"}:
            raise ValueError("execution_field must be open or close")
        self.portfolio = portfolio
        self.fee_model = fee_model
        self.market_rules = market_rules
        self.execution_field = execution_field

    def _intent(
        self,
        order: Order,
        price: float,
        pre_match_equity: float,
    ) -> _Intent:
        requested = float(order.requested or 0.0)
        position_amount = self.portfolio.amount(order.asset)
        sizing = order.sizing
        if sizing in {OrderSizing.QUANTITY, OrderSizing.TARGET_QUANTITY}:
            return _Intent(requested, None, order.cash_adaptive)

        if sizing is OrderSizing.VALUE:
            value = abs(requested)
            amount = value / price
            return _Intent(
                amount if requested > 0 else -amount,
                value if requested > 0 else None,
                True,
            )
        if sizing is OrderSizing.PERCENT:
            value = abs(requested) * pre_match_equity
            amount = value / price
            return _Intent(
                amount if requested > 0 else -amount,
                value if requested > 0 else None,
                True,
            )

        target_value = (
            requested
            if sizing is OrderSizing.TARGET_VALUE
            else requested * pre_match_equity
        )
        target_quantity = max(0.0, target_value) / price
        normalized_target = self.market_rules.normalize_buy(
            order.asset, target_quantity
        )
        difference = normalized_target - position_amount
        return _Intent(difference, None, True)

    def _affordable_buy_quantity(
        self,
        order: Order,
        requested: float,
        price: float,
        session: pd.Timestamp,
        spending_limit: float | None,
    ) -> float:
        quantity = self.market_rules.normalize_buy(order.asset, requested)
        if quantity <= 0:
            return 0.0
        available = self.portfolio.cash
        if spending_limit is not None:
            available = min(available, max(0.0, spending_limit))
        fees = self.fee_model.calculate(
            order.asset, quantity * price, is_sell=False, session=session
        )
        if quantity * price + fees.total <= available + _EPSILON:
            return quantity

        rough = max(0.0, available / price)
        quantity = self.market_rules.normalize_buy(order.asset, min(quantity, rough))
        step = (
            1.0
            if self.market_rules.allows_single_share_increment(order.asset)
            else 100.0
        )
        while quantity > 0:
            fees = self.fee_model.calculate(
                order.asset, quantity * price, is_sell=False, session=session
            )
            if quantity * price + fees.total <= available + _EPSILON:
                return quantity
            quantity = self.market_rules.normalize_buy(order.asset, quantity - step)
        return 0.0

    @staticmethod
    def _fallback_buy_direction(order: Order) -> bool:
        if order.sizing in {
            OrderSizing.TARGET_VALUE,
            OrderSizing.TARGET_PERCENT,
        }:
            return True
        return float(order.requested or 0.0) > 0

    @staticmethod
    def _cancel_below_minimum(order: Order, message: str) -> None:
        reason = (
            RejectReason.BELOW_MINIMUM_ORDER
            if order.is_batch and order.is_target
            else None
        )
        order.cancel(reason, message)

    def match(
        self,
        order: Order,
        bar: DailyBar | None,
        session: pd.Timestamp,
        pre_match_equity: float,
        active_position_count: int,
    ) -> MatchResult | None:
        if bar is None:
            failure = self.market_rules.validate_market(
                order.asset,
                bar,
                is_buy=self._fallback_buy_direction(order),
                execution_field=self.execution_field,
            )
            assert failure is not None
            order.reject(*failure)
            return None

        price = bar.value(self.execution_field)
        if not np.isfinite(price) or price <= 0:
            order.reject(RejectReason.NO_MARKET_DATA, "成交端点价格无效")
            return None
        intent = self._intent(order, price, pre_match_equity)
        if abs(intent.amount) <= _EPSILON:
            self._cancel_below_minimum(order, "目标仓位无需交易或不足最小交易单位")
            return None

        is_buy = intent.amount > 0
        failure = self.market_rules.validate_market(
            order.asset,
            bar,
            is_buy=is_buy,
            execution_field=self.execution_field,
        )
        if failure is not None:
            order.reject(*failure)
            return None

        position = self.portfolio.position(order.asset)
        position_amount = position.amount if position is not None else 0.0
        requested_quantity = abs(intent.amount)
        if is_buy and intent.adaptive:
            normalized = self.market_rules.normalize_buy(
                order.asset, requested_quantity
            )
            if normalized <= 0:
                order.resolve_amount(requested_quantity)
                self._cancel_below_minimum(
                    order, "目标买单不足证券最小交易单位，委托取消"
                )
                return None
            requested_quantity = normalized
        elif not is_buy and intent.adaptive:
            requested_quantity = self.market_rules.normalize_sell(
                order.asset, requested_quantity, position_amount
            )
            if requested_quantity <= 0:
                order.resolve_amount(-abs(intent.amount))
                self._cancel_below_minimum(order, "卖出金额不足有效交易单位，委托取消")
                return None

        signed_requested = requested_quantity if is_buy else -requested_quantity
        order.resolve_amount(signed_requested)
        failure = self.market_rules.validate_quantity(
            order.asset,
            requested_quantity,
            is_buy=is_buy,
            position_amount=position_amount,
        )
        if failure is not None:
            if order.is_batch and order.is_target and is_buy:
                self._cancel_below_minimum(
                    order, "目标买单不足证券最小交易单位，委托取消"
                )
            else:
                order.reject(*failure)
            return None

        if is_buy:
            opens_new_position = position is None or position_amount <= _EPSILON
            if (
                opens_new_position
                and order.position_limit is not None
                and active_position_count >= order.position_limit
            ):
                order.cancel(None, "组合已达到批量订单的持仓标的上限")
                return None
            if intent.adaptive:
                quantity = self._affordable_buy_quantity(
                    order,
                    requested_quantity,
                    price,
                    session,
                    intent.spending_limit,
                )
                if quantity <= 0:
                    self._cancel_below_minimum(
                        order, "D+1实际预算不足证券最小交易单位，委托取消"
                    )
                    return None
            else:
                quantity = requested_quantity
                fees = self.fee_model.calculate(
                    order.asset,
                    quantity * price,
                    is_sell=False,
                    session=session,
                )
                if quantity * price + fees.total > self.portfolio.cash + _EPSILON:
                    order.reject(
                        RejectReason.INSUFFICIENT_CASH,
                        "可用现金不足以完成最小交易单位",
                    )
                    return None
            fees = self.fee_model.calculate(
                order.asset,
                quantity * price,
                is_sell=False,
                session=session,
            )
            return MatchResult(quantity, price, fees)

        quantity = requested_quantity
        if position is None or position_amount + _EPSILON < quantity:
            order.reject(RejectReason.INSUFFICIENT_POSITION, "持仓数量不足，禁止卖空")
            return None
        sellable = position.sellable_amount(session)
        if sellable + _EPSILON < quantity:
            order.reject(
                RejectReason.T_PLUS_ONE,
                "可卖数量不足，证券尚未完成 T+1 交收",
            )
            return None
        fees = self.fee_model.calculate(
            order.asset,
            quantity * price,
            is_sell=True,
            session=session,
        )
        return MatchResult(-quantity, price, fees)
