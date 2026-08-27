"""Callback-scoped access to the active trading algorithm."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, Protocol

from ..broker.costs import ChinaFeeModel
from ..foundation.exceptions import NoActiveAlgorithm
from ..model.asset import Asset
from ..model.order import Order


class AlgorithmAPI(Protocol):
    def resolve_asset(self, value: Asset | str) -> Asset: ...

    def submit_order(self, asset: Asset | str, amount: float) -> Order: ...

    def submit_orders(self, requests: Mapping[Asset | str, float]) -> list[Order]: ...

    def submit_order_value(self, asset: Asset | str, value: float) -> Order | None: ...

    def submit_order_values(
        self, requests: Mapping[Asset | str, float]
    ) -> list[Order]: ...

    def submit_order_percent(
        self, asset: Asset | str, percent: float
    ) -> Order | None: ...

    def submit_order_percents(
        self, requests: Mapping[Asset | str, float]
    ) -> list[Order]: ...

    def submit_order_target(
        self, asset: Asset | str, target: float
    ) -> Order | None: ...

    def submit_order_targets(
        self,
        targets: Mapping[Asset | str, float],
        *,
        position_limit: int | None = None,
    ) -> list[Order]: ...

    def submit_order_target_value(
        self, asset: Asset | str, target: float
    ) -> Order | None: ...

    def submit_order_target_values(
        self,
        targets: Mapping[Asset | str, float],
        *,
        position_limit: int | None = None,
    ) -> list[Order]: ...

    def submit_order_target_percent(
        self, asset: Asset | str, target: float
    ) -> Order | None: ...

    def submit_order_target_percents(
        self,
        targets: Mapping[Asset | str, float],
        *,
        position_limit: int | None = None,
    ) -> list[Order]: ...

    def cancel_order(self, order: Order) -> None: ...

    def get_open_orders(self, asset: Asset | str | None = None) -> Any: ...

    def record(self, values: dict[str, Any]) -> None: ...

    def set_commission(self, model: ChinaFeeModel) -> None: ...

    @property
    def portfolio_value(self) -> float: ...


_ACTIVE_ALGORITHM: ContextVar[AlgorithmAPI | None] = ContextVar(
    "tualpha_active_algorithm", default=None
)


def active_algorithm() -> AlgorithmAPI:
    algorithm = _ACTIVE_ALGORITHM.get()
    if algorithm is None:
        raise NoActiveAlgorithm(
            "this API may only be called from initialize, handle_data, or analyze"
        )
    return algorithm


@contextmanager
def bind_algorithm(algorithm: AlgorithmAPI) -> Iterator[None]:
    token = _ACTIVE_ALGORITHM.set(algorithm)
    try:
        yield
    finally:
        _ACTIVE_ALGORITHM.reset(token)
