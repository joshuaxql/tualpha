"""Daily event loop and high-level run_algorithm entry point."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from tqdm.auto import tqdm

# Avoid a background monitor thread while native HDF5 readers are active on Windows.
tqdm.monitor_interval = 0

from .api import bind_algorithm
from .assets import Asset, AssetFinder
from .broker import SimulationBroker
from .bundle import acquire_bundle_read_lock, release_bundle_read_lock
from .calendar import ChinaTradingCalendar
from .config import (
    DEFAULT_BUNDLE_ROOT,
    AdjustmentMode,
    BacktestConfig,
    ExecutionTime,
    PlotlyJsMode,
)
from .costs import ChinaFeeModel
from .data import BarData, BundleDataPortal
from .exceptions import ConfigurationError, DataError, NoActiveAlgorithm
from .metrics import calculate_metrics
from .models import ClosedTrade, Order, Portfolio, Transaction
from .result import BacktestResult
from .rules import ChinaMarketRules

Initialize = Callable[[Any], None]
HandleData = Callable[[Any, BarData], None]
Analyze = Callable[[Any, BacktestResult], None]


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
        self._position_rows: list[dict[str, Any]] = []

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

    def submit_order(self, asset: Asset | str, amount: float) -> Order:
        session = self._require_order_phase()
        resolved = self.resolve_asset(asset)
        return self.broker.submit(
            resolved,
            amount,
            created_session=session,
            eligible_session=self._next_eligible_session(session),
        )

    def _raw_close(self, asset: Asset) -> float:
        if self.current_session is None:
            return np.nan
        return self.data_portal.value(
            asset, self.current_session, "close", adjusted=False
        )

    def submit_order_value(self, asset: Asset | str, value: float) -> Order | None:
        self._require_order_phase()
        resolved = self.resolve_asset(asset)
        price = self._raw_close(resolved)
        if not np.isfinite(price) or price <= 0 or abs(value) <= 1e-12:
            return None
        requested = abs(value) / price
        if value > 0:
            amount = self.market_rules.normalize_buy(resolved, requested)
        else:
            amount = -self.market_rules.normalize_sell(
                resolved, requested, self.portfolio.amount(resolved)
            )
        return self.submit_order(resolved, amount) if abs(amount) > 1e-12 else None

    def submit_order_target(self, asset: Asset | str, target: float) -> Order | None:
        self._require_order_phase()
        if target < 0:
            raise ValueError("negative target quantities would create a short position")
        resolved = self.resolve_asset(asset)
        current = self.portfolio.amount(resolved)
        difference = target - current
        if abs(difference) <= 1e-9:
            return None
        if difference > 0:
            amount = self.market_rules.normalize_buy(resolved, difference)
        else:
            amount = -self.market_rules.normalize_sell(
                resolved, abs(difference), current
            )
        return self.submit_order(resolved, amount) if abs(amount) > 1e-12 else None

    def submit_order_target_value(
        self, asset: Asset | str, target: float
    ) -> Order | None:
        self._require_order_phase()
        if target < 0:
            raise ValueError("negative target values would create a short position")
        resolved = self.resolve_asset(asset)
        price = self._raw_close(resolved)
        if not np.isfinite(price) or price <= 0:
            return None
        return self.submit_order_target(resolved, target / price)

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
            "handling_fee": fees.handling_fee,
            "transfer_fee": fees.transfer_fee,
            "included_handling_fee": fees.included_handling_fee,
            "fees": fees.total,
        }
        row.update(self._current_records)
        self._performance_rows.append(row)
        self._record_rows.append({"date": session, **self._current_records})
        self._capture_positions(session)

    def _capture_positions(self, session: pd.Timestamp) -> None:
        portfolio_value = self.portfolio.portfolio_value
        self._position_rows.append(
            {
                "date": session,
                "record_type": "CASH",
                "ts_code": "CASH",
                "name": "现金",
                "asset_type": "cash",
                "quantity": 0.0,
                "sellable_quantity": 0.0,
                "cost_basis": 1.0,
                "raw_close": 1.0,
                "adjusted_close": 1.0,
                "market_value": self.portfolio.cash,
                "weight": self.portfolio.cash / portfolio_value
                if portfolio_value
                else 0.0,
                "unrealized_pnl": 0.0,
                "cash": self.portfolio.cash,
                "portfolio_value": portfolio_value,
            }
        )
        position_items = list(self.broker.iter_positions())
        position_assets = [asset for asset, _ in position_items]
        adjusted_closes = self.data_portal.values(
            position_assets,
            session,
            ["close"],
            adjusted=True,
            reference_session=self.config.end,
        )["close"]
        for (asset, position), adjusted_close in zip(
            position_items, adjusted_closes, strict=True
        ):
            raw_close = position.last_sale_price
            if asset.delist_date is not None and session > asset.delist_date:
                raw_close = 0.0
            # CSV export is produced after the run, so it uses a conventional
            # end-normalized qfq series without exposing that denominator to callbacks.
            market_value = position.amount * raw_close
            self._position_rows.append(
                {
                    "date": session,
                    "record_type": "POSITION",
                    "ts_code": asset.ts_code,
                    "name": asset.name,
                    "asset_type": asset.asset_type.value,
                    "quantity": position.amount,
                    "sellable_quantity": position.sellable_amount(session),
                    "cost_basis": position.cost_basis,
                    "raw_close": raw_close,
                    "adjusted_close": adjusted_close,
                    "market_value": market_value,
                    "weight": market_value / portfolio_value
                    if portfolio_value
                    else 0.0,
                    "unrealized_pnl": market_value - position.total_cost,
                    "cash": self.portfolio.cash,
                    "portfolio_value": portfolio_value,
                }
            )

    @staticmethod
    def _orders_frame(orders: list[Order]) -> pd.DataFrame:
        columns = [
            "id",
            "created_session",
            "eligible_session",
            "ts_code",
            "asset_type",
            "amount",
            "filled",
            "average_price",
            "status",
            "reject_reason",
            "message",
        ]
        rows = [
            {
                "id": order.id,
                "created_session": order.created_session,
                "eligible_session": order.eligible_session,
                "ts_code": order.asset.ts_code,
                "asset_type": order.asset.asset_type.value,
                "amount": order.amount,
                "filled": order.filled,
                "average_price": order.average_price,
                "status": order.status.value,
                "reject_reason": order.reject_reason.value
                if order.reject_reason
                else "",
                "message": order.message,
            }
            for order in orders
        ]
        return pd.DataFrame(rows, columns=columns)

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
            "handling_fee",
            "transfer_fee",
            "fees",
        ]
        rows = [
            {
                "id": transaction.id,
                "order_id": transaction.order_id,
                "date": transaction.session,
                "ts_code": transaction.asset.ts_code,
                "asset_type": transaction.asset.asset_type.value,
                "amount": transaction.amount,
                "price": transaction.price,
                "gross_value": transaction.gross_value,
                "commission": transaction.fees.commission,
                "stamp_tax": transaction.fees.stamp_tax,
                "handling_fee": transaction.fees.handling_fee,
                "transfer_fee": transaction.fees.transfer_fee,
                "fees": transaction.fees.total,
            }
            for transaction in transactions
        ]
        return pd.DataFrame(rows, columns=columns)

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
        rows = [
            {
                "ts_code": trade.asset.ts_code,
                "asset_type": trade.asset.asset_type.value,
                "entry_date": trade.entry_session,
                "exit_date": trade.exit_session,
                "quantity": trade.quantity,
                "entry_price": trade.entry_price,
                "exit_price": trade.exit_price,
                "pnl": trade.pnl,
                "fees": trade.fees,
                "holding_days": trade.holding_days,
            }
            for trade in trades
        ]
        return pd.DataFrame(rows, columns=columns)

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
        positions = pd.DataFrame(self._position_rows)
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
    )
    return TradingAlgorithm(
        config,
        initialize=initialize,
        handle_data=handle_data,
        analyze=analyze,
        fee_model=fee_model,
    ).run()
