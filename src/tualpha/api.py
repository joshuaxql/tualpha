"""Zipline-style public functions bound to the active algorithm callback."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, Protocol

from .assets import Asset
from .costs import ChinaFeeModel
from .exceptions import NoActiveAlgorithm
from .models import Order


class _AlgorithmAPI(Protocol):
    def resolve_asset(self, value: Asset | str) -> Asset: ...

    def submit_order(self, asset: Asset | str, amount: float) -> Order: ...

    def submit_order_value(self, asset: Asset | str, value: float) -> Order | None: ...

    def submit_order_target(
        self, asset: Asset | str, target: float
    ) -> Order | None: ...

    def submit_order_target_value(
        self, asset: Asset | str, target: float
    ) -> Order | None: ...

    def cancel_order(self, order: Order) -> None: ...

    def get_open_orders(self, asset: Asset | str | None = None) -> Any: ...

    def record(self, values: dict[str, Any]) -> None: ...

    def set_commission(self, model: ChinaFeeModel) -> None: ...

    @property
    def portfolio_value(self) -> float: ...


_ACTIVE_ALGORITHM: ContextVar[_AlgorithmAPI | None] = ContextVar(
    "tualpha_active_algorithm", default=None
)


def _active() -> _AlgorithmAPI:
    algorithm = _ACTIVE_ALGORITHM.get()
    if algorithm is None:
        raise NoActiveAlgorithm(
            "this API may only be called from initialize, handle_data, or analyze"
        )
    return algorithm


@contextmanager
def bind_algorithm(algorithm: _AlgorithmAPI) -> Iterator[None]:
    token = _ACTIVE_ALGORITHM.set(algorithm)
    try:
        yield
    finally:
        _ACTIVE_ALGORITHM.reset(token)


def symbol(code: str) -> Asset:
    """Resolve a stock or ETF by Tushare code or unique six-digit symbol."""

    return _active().resolve_asset(code)


def order(asset: Asset | str, amount: float) -> Order:
    """Submit a signed share/ETF-unit market order for the next session."""

    return _active().submit_order(asset, amount)


def order_value(asset: Asset | str, value: float) -> Order | None:
    """Trade approximately a signed CNY value, rounded to a valid lot."""

    return _active().submit_order_value(asset, value)


def order_target(asset: Asset | str, target: float) -> Order | None:
    """Move a position toward a target quantity."""

    return _active().submit_order_target(asset, target)


def order_target_value(asset: Asset | str, target: float) -> Order | None:
    """Move a position toward a target raw market value."""

    return _active().submit_order_target_value(asset, target)


def order_percent(asset: Asset | str, percent: float) -> Order | None:
    """Trade a signed fraction of current portfolio value."""

    return order_value(asset, _active().portfolio_value * percent)


def order_target_percent(asset: Asset | str, target: float) -> Order | None:
    """Move a position toward a fraction of current portfolio value."""

    if target < 0:
        raise ValueError("negative targets would create a short position")
    return order_target_value(asset, _active().portfolio_value * target)


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
