"""Strategy-facing functions bound to the active algorithm callback."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from ..broker.costs import ChinaFeeModel
from ..core.execution_context import active_algorithm, bind_algorithm
from ..model.asset import Asset
from ..model.order import Order

__all__ = [
    "bind_algorithm",
    "cancel_order",
    "get_open_orders",
    "order",
    "order_many",
    "order_percent",
    "order_percent_many",
    "order_target",
    "order_target_many",
    "order_target_percent",
    "order_target_percent_many",
    "order_target_value",
    "order_target_value_many",
    "order_value",
    "order_value_many",
    "record",
    "set_commission",
    "symbol",
]


def _active():
    return active_algorithm()


def symbol(code: str) -> Asset:
    """Resolve a stock or ETF by Tushare code or unique six-digit symbol."""

    return _active().resolve_asset(code)


def order(asset: Asset | str, amount: float) -> Order:
    """Submit a signed quantity for execution at the next session endpoint."""

    return _active().submit_order(asset, amount)


def order_many(requests: Mapping[Asset | str, float]) -> list[Order]:
    """Submit mapping-ordered fixed-quantity orders."""

    return _active().submit_orders(requests)


def order_value(asset: Asset | str, value: float) -> Order | None:
    """Trade within a signed CNY limit at the next execution endpoint."""

    return _active().submit_order_value(asset, value)


def order_value_many(requests: Mapping[Asset | str, float]) -> list[Order]:
    """Submit mapping-ordered CNY-limited orders."""

    return _active().submit_order_values(requests)


def order_percent(asset: Asset | str, percent: float) -> Order | None:
    """Trade a signed fraction of D+1 pre-match portfolio equity."""

    return _active().submit_order_percent(asset, percent)


def order_percent_many(requests: Mapping[Asset | str, float]) -> list[Order]:
    """Submit mapping-ordered percent-of-equity orders."""

    return _active().submit_order_percents(requests)


def order_target(asset: Asset | str, target: float) -> Order | None:
    """Move a position toward a target quantity."""

    return _active().submit_order_target(asset, target)


def order_target_many(
    targets: Mapping[Asset | str, float], *, position_limit: int | None = None
) -> list[Order]:
    """Submit cash-adaptive batch quantity targets."""

    return _active().submit_order_targets(targets, position_limit=position_limit)


def order_target_value(asset: Asset | str, target: float) -> Order | None:
    """Move toward a CNY target using the D+1 execution price."""

    return _active().submit_order_target_value(asset, target)


def order_target_value_many(
    targets: Mapping[Asset | str, float], *, position_limit: int | None = None
) -> list[Order]:
    """Submit mapping-ordered target-value orders."""

    return _active().submit_order_target_values(targets, position_limit=position_limit)


def order_target_percent(asset: Asset | str, target: float) -> Order | None:
    """Move toward a D+1 pre-match equity weight."""

    return _active().submit_order_target_percent(asset, target)


def order_target_percent_many(
    targets: Mapping[Asset | str, float], *, position_limit: int | None = None
) -> list[Order]:
    """Submit mapping-ordered target-weight orders."""

    return _active().submit_order_target_percents(
        targets, position_limit=position_limit
    )


def cancel_order(order_to_cancel: Order) -> None:
    _active().cancel_order(order_to_cancel)


def get_open_orders(asset: Asset | str | None = None) -> Any:
    return _active().get_open_orders(asset)


def record(**values: Any) -> None:
    """Attach custom scalar values to the current daily performance row."""

    _active().record(values)


def set_commission(model: ChinaFeeModel) -> None:
    """Replace the fee model, normally during initialize."""

    _active().set_commission(model)
