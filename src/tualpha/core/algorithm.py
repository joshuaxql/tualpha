"""Daily event loop and high-level run_algorithm entry point."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from tqdm.auto import tqdm

# Avoid a background monitor thread during native DuckDB/Parquet reads on Windows.
tqdm.monitor_interval = 0

from ..analysis.metrics import calculate_metrics
from ..analysis.result import BacktestResult
from ..broker.costs import ChinaFeeModel
from ..broker.market_rules import ChinaMarketRules
from ..broker.simulation import SimulationBroker
from ..data.bar import BarData
from ..data.bundle.manager import acquire_bundle_read_lock, release_bundle_read_lock
from ..data.portal import BundleDataPortal
from ..data.trading_calendar import ChinaTradingCalendar
from ..foundation.config import (
    DEFAULT_BUNDLE_ROOT,
    AdjustmentMode,
    BacktestConfig,
    ExecutionTime,
    PlotlyJsMode,
)
from ..foundation.exceptions import ConfigurationError, DataError, NoActiveAlgorithm
from ..model.asset import Asset, AssetFinder
from ..model.order import Order, OrderSizing, Transaction
from ..model.portfolio import ClosedTrade, Portfolio
from .execution_context import bind_algorithm

Initialize = Callable[[Any], None]
HandleData = Callable[[Any, BarData], None]
Analyze = Callable[[Any, BacktestResult], None]

_POSITION_COLUMNS = (
    "date",
    "record_type",
    "ts_code",
    "name",
    "asset_type",
    "quantity",
    "sellable_quantity",
    "cost_basis",
    "raw_close",
    "adjusted_close",
    "asset_return",
    "market_value",
    "weight",
    "unrealized_pnl",
    "cash",
    "portfolio_value",
)


class AlgorithmContext:
    """Mutable user namespace with a live portfolio reference."""

    def __init__(self, portfolio: Portfolio) -> None:
        self.portfolio = portfolio
        self.datetime: pd.Timestamp | None = None


class TradingAlgorithm:
    """Coordinate local data, simulated execution, callbacks, and metrics."""

    def __init__(
        self,
        config: BacktestConfig,
        *,
        initialize: Initialize | None = None,
        handle_data: HandleData | None = None,
        analyze: Analyze | None = None,
        fee_model: ChinaFeeModel | None = None,
    ) -> None:
        self.config = config
        self.initialize_callback = initialize or (lambda context: None)
        self.handle_data_callback = handle_data or (lambda context, data: None)
        self.analyze_callback = analyze

        initialization_lock, _ = acquire_bundle_read_lock(
            config.bundle_root, config.bundle_name
        )
        try:
            self.asset_finder = AssetFinder(config.bundle_root, config.bundle_name)
            self.calendar = ChinaTradingCalendar(config.bundle_root, config.bundle_name)
            sessions = self.calendar.sessions_in_range(config.start, config.end)
            if sessions.empty:
                raise ConfigurationError("backtest range contains no trading sessions")
            if (
                config.start < self.calendar.first_session
                or config.end > self.calendar.last_session
            ):
                raise ConfigurationError(
                    f"backtest range must be within "
                    f"{self.calendar.first_session.date()} and "
                    f"{self.calendar.last_session.date()}"
                )
            self.sessions = sessions
            self.data_portal = BundleDataPortal(
                config.bundle_root,
                self.asset_finder,
                self.calendar,
                config.adjustment,
                config.end,
                config.bundle_name,
                column_cache_max_bytes=(
                    None
                    if config.column_cache_mib is None
                    else config.column_cache_mib * 1024**2
                ),
            )
        finally:
            release_bundle_read_lock(initialization_lock)
        self.data = BarData(self.data_portal)
        self.portfolio = Portfolio(config.capital_base)
        self.market_rules = ChinaMarketRules()
        self.broker = SimulationBroker(
            self.portfolio,
            self.calendar,
            self.data_portal,
            fee_model or ChinaFeeModel(),
            self.market_rules,
            config.execution_time.value,
        )
        self.context = AlgorithmContext(self.portfolio)
        self.current_session: pd.Timestamp | None = None
        self._phase = "created"
        self._current_records: dict[str, Any] = {}
        self._record_rows: list[dict[str, Any]] = []
        self._performance_rows: list[dict[str, Any]] = []
        self._position_columns: dict[str, list[Any]] = {
            column: [] for column in _POSITION_COLUMNS
        }

    @property
    def portfolio_value(self) -> float:
        return self.portfolio.portfolio_value

    def resolve_asset(self, value: Asset | str) -> Asset:
        if isinstance(value, Asset):
            return value
        return self.asset_finder.retrieve_asset(value)

    def _require_order_phase(self) -> pd.Timestamp:
        if self._phase != "handle_data" or self.current_session is None:
            raise NoActiveAlgorithm("orders may only be submitted from handle_data")
        return self.current_session

    def _next_eligible_session(self, session: pd.Timestamp) -> pd.Timestamp:
        try:
            return self.calendar.next_session(session)
        except DataError:
            return session + pd.Timedelta(days=1)

    def _submit_intent(
        self,
        asset: Asset | str,
        requested: float,
        sizing: OrderSizing,
        *,
        cash_adaptive: bool = False,
        is_target: bool = False,
        position_limit: int | None = None,
    ) -> Order:
        session = self._require_order_phase()
        return self.broker.submit(
            self.resolve_asset(asset),
            requested,
            created_session=session,
            eligible_session=self._next_eligible_session(session),
            sizing=sizing,
            position_limit=position_limit,
            cash_adaptive=cash_adaptive,
            is_target=is_target,
        )

    def _submit_intents(
        self,
        prepared: list[tuple[Asset, float, OrderSizing, bool]],
        *,
        position_limit: int | None = None,
        cash_adaptive: bool = False,
    ) -> list[Order]:
        session = self._require_order_phase()
        if position_limit is not None and position_limit <= 0:
            raise ValueError("position_limit must be positive")
        if not prepared:
            return []
        return self.broker.submit_many(
            prepared,
            created_session=session,
            eligible_session=self._next_eligible_session(session),
            position_limit=position_limit,
            cash_adaptive=cash_adaptive,
        )

    def submit_order(self, asset: Asset | str, amount: float) -> Order:
        return self._submit_intent(asset, amount, OrderSizing.QUANTITY)

    def submit_order_value(self, asset: Asset | str, value: float) -> Order | None:
        self._require_order_phase()
        if abs(value) <= 1e-12:
            return None
        return self._submit_intent(
            asset,
            value,
            OrderSizing.VALUE,
            cash_adaptive=True,
        )

    def submit_order_percent(self, asset: Asset | str, percent: float) -> Order | None:
        self._require_order_phase()
        if percent < -1 or percent > 1:
            raise ValueError("order percent must be between -1 and 1")
        if abs(percent) <= 1e-12:
            return None
        return self._submit_intent(
            asset,
            percent,
            OrderSizing.PERCENT,
            cash_adaptive=True,
        )

    def submit_order_target(self, asset: Asset | str, target: float) -> Order | None:
        self._require_order_phase()
        if target < 0:
            raise ValueError("negative target quantities would create a short position")
        resolved = self.resolve_asset(asset)
        current = self.portfolio.amount(resolved)
        difference = float(target) - current
        if abs(difference) <= 1e-9:
            return None
        if difference > 0:
            amount = self.market_rules.normalize_buy(resolved, difference)
        else:
            amount = -self.market_rules.normalize_sell(
                resolved, abs(difference), current
            )
        if abs(amount) <= 1e-12:
            return None
        return self._submit_intent(
            resolved,
            amount,
            OrderSizing.TARGET_QUANTITY,
            is_target=True,
        )

    def submit_order_target_value(
        self, asset: Asset | str, target: float
    ) -> Order | None:
        self._require_order_phase()
        if target < 0:
            raise ValueError("negative target values would create a short position")
        resolved = self.resolve_asset(asset)
        if target <= 1e-12 and self.portfolio.amount(resolved) <= 1e-12:
            return None
        return self._submit_intent(
            resolved,
            target,
            OrderSizing.TARGET_VALUE,
            cash_adaptive=True,
            is_target=True,
        )

    def submit_order_target_percent(
        self, asset: Asset | str, target: float
    ) -> Order | None:
        self._require_order_phase()
        if target < 0 or target > 1:
            raise ValueError("target percent must be between 0 and 1")
        resolved = self.resolve_asset(asset)
        if target <= 1e-12 and self.portfolio.amount(resolved) <= 1e-12:
            return None
        return self._submit_intent(
            resolved,
            target,
            OrderSizing.TARGET_PERCENT,
            cash_adaptive=True,
            is_target=True,
        )

    def submit_orders(self, requests: Mapping[Asset | str, float]) -> list[Order]:
        prepared = [
            (self.resolve_asset(asset), float(amount), OrderSizing.QUANTITY, False)
            for asset, amount in requests.items()
        ]
        return self._submit_intents(prepared)

    def submit_order_values(self, requests: Mapping[Asset | str, float]) -> list[Order]:
        prepared = [
            (self.resolve_asset(asset), float(value), OrderSizing.VALUE, False)
            for asset, value in requests.items()
            if abs(float(value)) > 1e-12
        ]
        return self._submit_intents(prepared, cash_adaptive=True)

    def submit_order_percents(
        self, requests: Mapping[Asset | str, float]
    ) -> list[Order]:
        if any(percent < -1 or percent > 1 for percent in requests.values()):
            raise ValueError("order percents must be between -1 and 1")
        prepared = [
            (self.resolve_asset(asset), float(percent), OrderSizing.PERCENT, False)
            for asset, percent in requests.items()
            if abs(float(percent)) > 1e-12
        ]
        return self._submit_intents(prepared, cash_adaptive=True)

    def submit_order_targets(
        self,
        targets: Mapping[Asset | str, float],
        *,
        position_limit: int | None = None,
    ) -> list[Order]:
        if any(target < 0 for target in targets.values()):
            raise ValueError("negative target quantities would create a short position")
        prepared: list[tuple[Asset, float, OrderSizing, bool]] = []
        for asset, target in targets.items():
            resolved = self.resolve_asset(asset)
            current = self.portfolio.amount(resolved)
            difference = float(target) - current
            if abs(difference) > 1e-9:
                prepared.append(
                    (resolved, difference, OrderSizing.TARGET_QUANTITY, True)
                )
        return self._submit_intents(
            prepared,
            position_limit=position_limit,
            cash_adaptive=True,
        )

    def submit_order_target_values(
        self,
        targets: Mapping[Asset | str, float],
        *,
        position_limit: int | None = None,
    ) -> list[Order]:
        if any(target < 0 for target in targets.values()):
            raise ValueError("negative target values would create a short position")
        prepared: list[tuple[Asset, float, OrderSizing, bool]] = []
        for asset, target in targets.items():
            resolved = self.resolve_asset(asset)
            if target > 1e-12 or self.portfolio.amount(resolved) > 1e-12:
                prepared.append(
                    (resolved, float(target), OrderSizing.TARGET_VALUE, True)
                )
        return self._submit_intents(
            prepared,
            position_limit=position_limit,
            cash_adaptive=True,
        )

    def submit_order_target_percents(
        self,
        targets: Mapping[Asset | str, float],
        *,
        position_limit: int | None = None,
    ) -> list[Order]:
        if any(target < 0 or target > 1 for target in targets.values()):
            raise ValueError("target percents must be between 0 and 1")
        prepared: list[tuple[Asset, float, OrderSizing, bool]] = []
        for asset, target in targets.items():
            resolved = self.resolve_asset(asset)
            if target > 1e-12 or self.portfolio.amount(resolved) > 1e-12:
                prepared.append(
                    (resolved, float(target), OrderSizing.TARGET_PERCENT, True)
                )
        return self._submit_intents(
            prepared,
            position_limit=position_limit,
            cash_adaptive=True,
        )

    def cancel_order(self, order: Order) -> None:
        self.broker.cancel(order)

    def get_open_orders(
        self, asset: Asset | str | None = None
    ) -> list[Order] | dict[Asset, list[Order]]:
        if asset is not None:
            return self.broker.get_open_orders(self.resolve_asset(asset))
        grouped: dict[Asset, list[Order]] = defaultdict(list)
        for order in self.broker.get_open_orders():
            grouped[order.asset].append(order)
        return dict(grouped)

    def record(self, values: dict[str, Any]) -> None:
        if self._phase != "handle_data":
            raise NoActiveAlgorithm("record may only be called from handle_data")
        reserved = set(values).intersection(
            {
                "cash",
                "positions_value",
                "portfolio_value",
                "returns",
                "algorithm_period_return",
            }
        )
        if reserved:
            raise ValueError(
                f"record keys conflict with performance columns: {sorted(reserved)}"
            )
        self._current_records.update(values)

    def set_commission(self, model: ChinaFeeModel) -> None:
        if not isinstance(model, ChinaFeeModel):
            raise TypeError("commission model must be a ChinaFeeModel")
        self.broker.fee_model = model

    def _capture_session(
        self,
        session: pd.Timestamp,
        previous_value: float,
        transaction_start: int,
    ) -> None:
        fees = self.broker.fees_for_session(session)
        daily_transactions = self.broker.transactions[transaction_start:]
        turnover_value = sum(
            transaction.gross_value for transaction in daily_transactions
        )
        value = self.portfolio.portfolio_value
        daily_return = value / previous_value - 1.0 if previous_value else 0.0
        row = {
            "date": session,
            "cash": self.portfolio.cash,
            "positions_value": self.portfolio.positions_value,
            "portfolio_value": value,
            "pnl": value - self.config.capital_base,
            "returns": daily_return,
            "algorithm_period_return": value / self.config.capital_base - 1.0,
            "gross_exposure": (
                self.portfolio.positions_value / value if value else 0.0
            ),
            "turnover": turnover_value / previous_value if previous_value else 0.0,
            "commission": fees.commission,
            "stamp_tax": fees.stamp_tax,
            "transfer_fee": fees.transfer_fee,
            "fees": fees.total,
        }
        row.update(self._current_records)
        self._performance_rows.append(row)
        self._record_rows.append({"date": session, **self._current_records})
        self._capture_positions(session)

    def _capture_positions(self, session: pd.Timestamp) -> None:
        portfolio_value = self.portfolio.portfolio_value
        cash = self.portfolio.cash
        columns = self._position_columns
        dates = columns["date"]
        record_types = columns["record_type"]
        codes = columns["ts_code"]
        names = columns["name"]
        asset_types = columns["asset_type"]
        quantities = columns["quantity"]
        sellable_quantities = columns["sellable_quantity"]
        cost_bases = columns["cost_basis"]
        raw_closes = columns["raw_close"]
        adjusted_values = columns["adjusted_close"]
        asset_returns = columns["asset_return"]
        market_values = columns["market_value"]
        weights = columns["weight"]
        unrealized_values = columns["unrealized_pnl"]
        cash_values = columns["cash"]
        portfolio_values = columns["portfolio_value"]

        dates.append(session)
        record_types.append("CASH")
        codes.append("CASH")
        names.append("现金")
        asset_types.append("cash")
        quantities.append(0.0)
        sellable_quantities.append(0.0)
        cost_bases.append(1.0)
        raw_closes.append(1.0)
        adjusted_values.append(1.0)
        asset_returns.append(0.0)
        market_values.append(cash)
        weights.append(cash / portfolio_value if portfolio_value else 0.0)
        unrealized_values.append(0.0)
        cash_values.append(cash)
        portfolio_values.append(portfolio_value)

        position_items = list(self.broker.iter_positions())
        if not position_items:
            return
        position_assets = [asset for asset, _ in position_items]
        positions = [position for _, position in position_items]
        self.data_portal.prepare_position_data(position_assets)
        position_prices = self.data_portal.values(
            position_assets,
            session,
            ["close", "pre_close"],
            adjusted=True,
            reference_session=self.config.end,
        )
        adjusted_closes = position_prices["close"]
        pre_closes = position_prices["pre_close"]
        # CSV export is produced after the run, so adjusted closes use a
        # conventional end-normalized qfq denominator not exposed to callbacks.
        amounts = np.fromiter(
            (position.amount for position in positions),
            dtype=float,
            count=len(positions),
        )
        total_costs = np.fromiter(
            (position.total_cost for position in positions),
            dtype=float,
            count=len(positions),
        )
        raw_prices = np.fromiter(
            (
                0.0
                if asset.delist_date is not None and session > asset.delist_date
                else position.last_sale_price
                for asset, position in position_items
            ),
            dtype=float,
            count=len(positions),
        )
        sellable = np.fromiter(
            (position.sellable_amount(session) for position in positions),
            dtype=float,
            count=len(positions),
        )
        daily_returns = (
            np.divide(
                raw_prices,
                pre_closes,
                out=np.ones(len(positions), dtype=float),
                where=np.isfinite(pre_closes) & (pre_closes > 0) & (raw_prices > 0),
            )
            - 1.0
        )
        position_values = amounts * raw_prices
        position_weights = (
            position_values / portfolio_value
            if portfolio_value
            else np.zeros(len(positions), dtype=float)
        )
        bases = np.divide(
            total_costs,
            amounts,
            out=np.zeros(len(positions), dtype=float),
            where=amounts != 0,
        )
        count = len(positions)
        dates.extend([session] * count)
        record_types.extend(["POSITION"] * count)
        codes.extend(asset.ts_code for asset in position_assets)
        names.extend(asset.name for asset in position_assets)
        asset_types.extend(asset.asset_type.value for asset in position_assets)
        quantities.extend(amounts)
        sellable_quantities.extend(sellable)
        cost_bases.extend(bases)
        raw_closes.extend(raw_prices)
        adjusted_values.extend(adjusted_closes)
        asset_returns.extend(daily_returns)
        market_values.extend(position_values)
        weights.extend(position_weights)
        unrealized_values.extend(position_values - total_costs)
        cash_values.extend([cash] * count)
        portfolio_values.extend([portfolio_value] * count)

    @staticmethod
    def _orders_frame(orders: list[Order]) -> pd.DataFrame:
        columns = [
            "id",
            "created_session",
            "eligible_session",
            "ts_code",
            "asset_type",
            "sizing",
            "requested",
            "is_batch",
            "is_target",
            "amount",
            "filled",
            "average_price",
            "status",
            "reject_reason",
            "message",
        ]
        return pd.DataFrame(
            {
                "id": [order.id for order in orders],
                "created_session": [order.created_session for order in orders],
                "eligible_session": [order.eligible_session for order in orders],
                "ts_code": [order.asset.ts_code for order in orders],
                "asset_type": [order.asset.asset_type.value for order in orders],
                "sizing": [order.sizing.value for order in orders],
                "requested": [order.requested for order in orders],
                "is_batch": [order.is_batch for order in orders],
                "is_target": [order.is_target for order in orders],
                "amount": [order.amount for order in orders],
                "filled": [order.filled for order in orders],
                "average_price": [order.average_price for order in orders],
                "status": [order.status.value for order in orders],
                "reject_reason": [
                    order.reject_reason.value if order.reject_reason else ""
                    for order in orders
                ],
                "message": [order.message for order in orders],
            },
            columns=columns,
        )

    @staticmethod
    def _transactions_frame(transactions: list[Transaction]) -> pd.DataFrame:
        columns = [
            "id",
            "order_id",
            "date",
            "ts_code",
            "asset_type",
            "amount",
            "price",
            "gross_value",
            "commission",
            "stamp_tax",
            "transfer_fee",
            "fees",
        ]
        return pd.DataFrame(
            {
                "id": [transaction.id for transaction in transactions],
                "order_id": [transaction.order_id for transaction in transactions],
                "date": [transaction.session for transaction in transactions],
                "ts_code": [transaction.asset.ts_code for transaction in transactions],
                "asset_type": [
                    transaction.asset.asset_type.value for transaction in transactions
                ],
                "amount": [transaction.amount for transaction in transactions],
                "price": [transaction.price for transaction in transactions],
                "gross_value": [
                    transaction.gross_value for transaction in transactions
                ],
                "commission": [
                    transaction.fees.commission for transaction in transactions
                ],
                "stamp_tax": [
                    transaction.fees.stamp_tax for transaction in transactions
                ],
                "transfer_fee": [
                    transaction.fees.transfer_fee for transaction in transactions
                ],
                "fees": [transaction.fees.total for transaction in transactions],
            },
            columns=columns,
        )

    @staticmethod
    def _closed_trades_frame(trades: list[ClosedTrade]) -> pd.DataFrame:
        columns = [
            "ts_code",
            "asset_type",
            "entry_date",
            "exit_date",
            "quantity",
            "entry_price",
            "exit_price",
            "pnl",
            "fees",
            "holding_days",
        ]
        return pd.DataFrame(
            {
                "ts_code": [trade.asset.ts_code for trade in trades],
                "asset_type": [trade.asset.asset_type.value for trade in trades],
                "entry_date": [trade.entry_session for trade in trades],
                "exit_date": [trade.exit_session for trade in trades],
                "quantity": [trade.quantity for trade in trades],
                "entry_price": [trade.entry_price for trade in trades],
                "exit_price": [trade.exit_price for trade in trades],
                "pnl": [trade.pnl for trade in trades],
                "fees": [trade.fees for trade in trades],
                "holding_days": [trade.holding_days for trade in trades],
            },
            columns=columns,
        )

    def run(self) -> BacktestResult:
        try:
            return self._run()
        finally:
            self.data_portal.close()

    def _run(self) -> BacktestResult:
        with bind_algorithm(self):
            self._phase = "initialize"
            self.initialize_callback(self.context)

        previous_session: pd.Timestamp | None = None
        previous_value = self.config.capital_base
        with tqdm(
            self.sessions,
            total=len(self.sessions),
            desc=f"TuAlpha 回测：{self.config.strategy_name}",
            unit="交易日",
            dynamic_ncols=True,
            disable=not self.config.show_progress,
        ) as progress:
            for session in progress:
                progress.set_postfix_str(session.strftime("%Y-%m-%d"), refresh=False)
                self.current_session = session
                self.context.datetime = session
                self.data._set_session(session)
                self._current_records = {}
                self.broker.apply_corporate_actions(previous_session, session)
                transaction_start = len(self.broker.transactions)
                self.broker.process_orders(session)
                self.broker.mark_to_market(session)

                with bind_algorithm(self):
                    self._phase = "handle_data"
                    self.handle_data_callback(self.context, self.data)
                # Callback orders cannot fill until a later session, so the
                # pre-callback valuation remains current for end-of-day capture.
                self._capture_session(session, previous_value, transaction_start)
                previous_value = self.portfolio.portfolio_value
                previous_session = session

        self.broker.cancel_remaining()
        self._phase = "finished"
        performance = pd.DataFrame(self._performance_rows).set_index("date")
        performance.index = pd.DatetimeIndex(performance.index, name="date")
        positions = pd.DataFrame(self._position_columns, columns=_POSITION_COLUMNS)
        orders = self._orders_frame(self.broker.orders)
        transactions = self._transactions_frame(self.broker.transactions)
        closed_trades = self._closed_trades_frame(self.broker.closed_trades)
        records = pd.DataFrame(self._record_rows)
        if not records.empty:
            records = records.set_index("date")

        if self.config.benchmark:
            benchmark_returns = self.data_portal.benchmark_returns(
                self.config.benchmark, performance.index
            )
            performance["benchmark_returns"] = benchmark_returns
            performance["benchmark_period_return"] = (
                1.0 + benchmark_returns
            ).cumprod() - 1.0

        metrics = calculate_metrics(
            performance,
            closed_trades,
            positions,
            self.config.annualization_factor,
        )
        result = BacktestResult(
            config=self.config,
            performance=performance,
            daily_positions=positions,
            orders=orders,
            transactions=transactions,
            closed_trades=closed_trades,
            metrics=metrics,
            records=records,
        )
        if self.analyze_callback is not None:
            with bind_algorithm(self):
                self._phase = "analyze"
                self.analyze_callback(self.context, result)
        self._phase = "finished"
        if self.config.output_dir is not None:
            result.export()
        return result


def run_algorithm(
    *,
    start: str | pd.Timestamp,
    end: str | pd.Timestamp,
    initialize: Initialize | None = None,
    handle_data: HandleData | None = None,
    analyze: Analyze | None = None,
    capital_base: float = 1_000_000.0,
    bundle_root: str | Path = DEFAULT_BUNDLE_ROOT,
    adjustment: AdjustmentMode | str = AdjustmentMode.QFQ,
    execution_time: ExecutionTime | str = ExecutionTime.OPEN,
    output_dir: str | Path | None = None,
    benchmark: str | None = None,
    strategy_name: str = "TuAlpha 回测策略",
    generate_report: bool = True,
    show_progress: bool = True,
    plotly_js: PlotlyJsMode | str = PlotlyJsMode.INLINE,
    bundle_name: str = "tualpha",
    column_cache_mib: int | None = None,
    fee_model: ChinaFeeModel | None = None,
) -> BacktestResult:
    """Run a point-in-time daily stock/ETF backtest.

    Set ``show_progress=False`` to disable the default tqdm session progress bar.
    """

    config = BacktestConfig(
        start=start,
        end=end,
        capital_base=capital_base,
        bundle_root=bundle_root,
        adjustment=adjustment,
        execution_time=execution_time,
        output_dir=output_dir,
        benchmark=benchmark,
        strategy_name=strategy_name,
        generate_report=generate_report,
        show_progress=show_progress,
        plotly_js=plotly_js,
        bundle_name=bundle_name,
        column_cache_mib=column_cache_mib,
    )
    return TradingAlgorithm(
        config,
        initialize=initialize,
        handle_data=handle_data,
        analyze=analyze,
        fee_model=fee_model,
    ).run()
