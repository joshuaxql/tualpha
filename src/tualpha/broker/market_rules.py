"""A-share market microstructure rules used by the simulated broker."""

from __future__ import annotations

import math

import numpy as np

from .assets import Asset, Board
from .data import DailyBar
from .models import RejectReason

_EPSILON = 1e-6


class ChinaMarketRules:
    """Validate price limits, suspensions, and board-lot quantities."""

    @staticmethod
    def minimum_order(asset: Asset) -> int:
        return 200 if asset.board is Board.STAR else 100

    @staticmethod
    def allows_single_share_increment(asset: Asset) -> bool:
        return asset.board in {Board.STAR, Board.BSE}

    def normalize_buy(self, asset: Asset, requested: float) -> float:
        quantity = math.floor(max(0.0, requested) + _EPSILON)
        minimum = self.minimum_order(asset)
        if quantity < minimum:
            return 0.0
        if self.allows_single_share_increment(asset):
            return float(quantity)
        return float(quantity // 100 * 100)

    def normalize_sell(
        self, asset: Asset, requested: float, position_amount: float
    ) -> float:
        quantity = min(max(0.0, requested), max(0.0, position_amount))
        if position_amount - quantity <= _EPSILON:
            return float(position_amount)
        minimum = self.minimum_order(asset)
        integer_quantity = math.floor(quantity + _EPSILON)
        if integer_quantity < minimum:
            return 0.0
        if self.allows_single_share_increment(asset):
            return float(integer_quantity)
        return float(integer_quantity // 100 * 100)

    def validate_quantity(
        self,
        asset: Asset,
        quantity: float,
        *,
        is_buy: bool,
        position_amount: float = 0.0,
    ) -> tuple[RejectReason, str] | None:
        if quantity <= _EPSILON:
            return RejectReason.ZERO_AMOUNT, "委托数量必须大于 0"
        minimum = self.minimum_order(asset)

        if not is_buy and abs(quantity - position_amount) <= _EPSILON:
            return None  # Full liquidation may contain an odd-lot remainder.

        rounded = round(quantity)
        if abs(quantity - rounded) > _EPSILON:
            return RejectReason.INVALID_LOT, "除一次性清仓外，委托数量必须为整数"
        integer_quantity = int(rounded)
        if integer_quantity < minimum:
            return (
                RejectReason.INVALID_LOT,
                f"{asset.ts_code} 单笔委托不得少于 {minimum} 股/份",
            )
        if not self.allows_single_share_increment(asset) and integer_quantity % 100:
            return RejectReason.INVALID_LOT, "该证券委托数量必须为 100 股/份的整数倍"
        return None

    def validate_market(
        self,
        asset: Asset,
        bar: DailyBar | None,
        *,
        is_buy: bool,
        execution_field: str,
    ) -> tuple[RejectReason, str] | None:
        if bar is None:
            return RejectReason.NO_MARKET_DATA, "当日无可用行情"
        if bar.suspended:
            return RejectReason.SUSPENDED, "证券当日停牌"
        if bar.volume <= 0:
            return RejectReason.ZERO_VOLUME, "证券当日成交量为零"

        price = bar.value(execution_field)
        tolerance = asset.price_tick / 2 + 1e-12
        if is_buy and np.isfinite(bar.up_limit) and price >= bar.up_limit - tolerance:
            return RejectReason.LIMIT_UP, f"{execution_field} 价格达到涨停价，禁止买入"
        if (
            not is_buy
            and np.isfinite(bar.down_limit)
            and price <= bar.down_limit + tolerance
        ):
            return (
                RejectReason.LIMIT_DOWN,
                f"{execution_field} 价格达到跌停价，禁止卖出",
            )
        return None
