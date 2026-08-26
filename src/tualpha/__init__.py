"""TuAlpha: a daily A-share stock and ETF backtesting framework."""

from .api import (
    cancel_order,
    get_open_orders,
    order,
    order_many,
    order_percent,
    order_percent_many,
    order_target,
    order_target_many,
    order_target_percent,
    order_target_percent_many,
    order_target_value,
    order_target_value_many,
    order_value,
    order_value_many,
    record,
    set_commission,
    symbol,
)
from .assets import Asset, AssetFinder, AssetType, Board
from .config import AdjustmentMode, BacktestConfig, ExecutionTime, PlotlyJsMode
from .costs import ChinaFeeModel, RateSchedule
from .data.query import LocalDataClient, local_data
from .engine import AlgorithmContext, TradingAlgorithm, run_algorithm
from .exceptions import (
    ConfigurationError,
    DataError,
    NoActiveAlgorithm,
    SymbolNotFound,
    TualphaError,
)
from .models import (
    Order,
    OrderSizing,
    OrderStatus,
    Portfolio,
    RejectReason,
    Transaction,
)
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
    "LocalDataClient",
    "NoActiveAlgorithm",
    "Order",
    "OrderSizing",
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
    "local_data",
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
    "run_algorithm",
    "set_commission",
    "symbol",
]

__version__ = "1.3.3"


def main() -> None:
    """Backward-compatible console entry point."""

    from .cli import main as cli_main

    cli_main()
