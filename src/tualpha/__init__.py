"""TuAlpha: a daily A-share stock and ETF backtesting framework."""

from .analysis.factor import (
    FactorAnalysisResult,
    analyze_factor_data,
    neutralize_factor_values,
    run_factor_analysis,
)
from .analysis.result import BacktestResult
from .apis import (
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
from .broker.costs import ChinaFeeModel, RateSchedule
from .core.algorithm import AlgorithmContext, TradingAlgorithm, run_algorithm
from .data.query import LocalDataClient, local_data
from .data.research import FactorData, factor_data
from .foundation.config import (
    AdjustmentMode,
    BacktestConfig,
    ExecutionTime,
    PlotlyJsMode,
)
from .foundation.exceptions import (
    ConfigurationError,
    DataError,
    NoActiveAlgorithm,
    SymbolNotFound,
    TualphaError,
)
from .model import (
    Order,
    OrderSizing,
    OrderStatus,
    Portfolio,
    RejectReason,
    Transaction,
)

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
    "FactorAnalysisResult",
    "FactorData",
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
    "analyze_factor_data",
    "cancel_order",
    "factor_data",
    "get_open_orders",
    "local_data",
    "neutralize_factor_values",
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
    "run_factor_analysis",
    "set_commission",
    "symbol",
]

__version__ = "2.0.0"


def main() -> None:
    """Backward-compatible console entry point."""

    from .cli import main as cli_main

    cli_main()
