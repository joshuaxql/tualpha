"""TuAlpha: a daily A-share stock and ETF backtesting framework."""

from .api import (
    cancel_order,
    get_open_orders,
    order,
    order_percent,
    order_target,
    order_target_many,
    order_target_percent,
    order_target_value,
    order_value,
    record,
    set_commission,
    symbol,
)
from .assets import Asset, AssetFinder, AssetType, Board
from .config import AdjustmentMode, BacktestConfig, ExecutionTime, PlotlyJsMode
from .costs import ChinaFeeModel, RateSchedule
from .engine import AlgorithmContext, TradingAlgorithm, run_algorithm
from .exceptions import (
    ConfigurationError,
    DataError,
    NoActiveAlgorithm,
    SymbolNotFound,
    TualphaError,
)
from .models import Order, OrderStatus, Portfolio, RejectReason, Transaction
from .result import BacktestResult

__all__ = [
    "AdjustmentMode",
    "AlgorithmContext",
    "Asset",
    "AssetFinder",
    "AssetType",
    "BacktestConfig",
    "BacktestResult",
    "Board",
    "ChinaFeeModel",
    "ConfigurationError",
    "DataError",
    "ExecutionTime",
    "NoActiveAlgorithm",
    "Order",
    "OrderStatus",
    "PlotlyJsMode",
    "Portfolio",
    "RateSchedule",
    "RejectReason",
    "SymbolNotFound",
    "TradingAlgorithm",
    "Transaction",
    "TualphaError",
    "cancel_order",
    "get_open_orders",
    "order",
    "order_percent",
    "order_target",
    "order_target_many",
    "order_target_percent",
    "order_target_value",
    "order_value",
    "record",
    "run_algorithm",
    "set_commission",
    "symbol",
]

__version__ = "0.8.1"


def main() -> None:
    """Backward-compatible console entry point."""

    from .cli import main as cli_main

    cli_main()
