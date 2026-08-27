"""国证 2000 多因子指数增强与 TuAlpha 性能基准。

该策略用于验证大横截面查询、PIT 财务查询和批量下单性能，不构成投资建议。
"""

from __future__ import annotations

import argparse
import json
import platform
from pathlib import Path
from time import perf_counter

import numpy as np
import pandas as pd

import tualpha
from tualpha import order_target_percent_many, record, run_algorithm

START_DATE = "2017-01-01"
END_DATE = "2026-08-26"
INDEX_CODE = "399303.SZ"
LOOKBACK = 61
MIN_HISTORY = 55
HOLD_COUNT = 30
TOTAL_EXPOSURE = 0.95
MIN_DAILY_TURNOVER = 10_000_000.0
ACTIVE_TILT = 0.50
WEIGHT_TOLERANCE = 0.0002

CURRENT_FIELDS = (
    "close",
    "volume",
    "turnover",
    "daily_basic.pe_ttm",
    "stock_st.is_st",
    "suspended",
    "industry.l1_name",
)
FUNDAMENTAL_FIELDS = ("fina_indicator.roe",)


def initialize(context) -> None:
    context.last_rebalance_month = None
    context.fields_checked = False


def _check_fields(data) -> None:
    available = set(data.available_fields())
    # 基础 OHLCV/停牌字段属于稳定的标准接口，不在 available_fields() 中枚举。
    required = {field for field in CURRENT_FIELDS if "." in field} | set(
        FUNDAMENTAL_FIELDS
    )
    missing = sorted(required - available)
    if missing:
        raise RuntimeError(f"当前 Bundle 缺少策略字段：{', '.join(missing)}")


def _factor_frame(data, members: pd.DataFrame) -> pd.DataFrame:
    mapped = members.loc[members["asset"].notna()].copy()
    if mapped.empty:
        return pd.DataFrame()

    assets = mapped["asset"].tolist()
    codes = [asset.ts_code for asset in assets]
    current = data.current_arrays(assets, CURRENT_FIELDS)
    fundamentals = data.fundamental_arrays(assets, FUNDAMENTAL_FIELDS)
    prices = data.history(assets, "close", LOOKBACK).reindex(columns=codes)

    raw_prices = prices.to_numpy(dtype=float)
    filled_prices = prices.ffill().to_numpy(dtype=float)
    history_count = np.isfinite(raw_prices).sum(axis=0)
    with np.errstate(divide="ignore", invalid="ignore"):
        momentum = filled_prices[-1] / filled_prices[0] - 1.0
        log_returns = np.diff(np.log(filled_prices), axis=0)
    volatility = np.full(len(assets), np.nan, dtype=float)
    enough_history = history_count >= MIN_HISTORY
    volatility[enough_history] = np.nanstd(
        log_returns[:, enough_history], axis=0, ddof=1
    ) * np.sqrt(252.0)

    close = np.asarray(current["close"], dtype=float)
    volume = np.asarray(current["volume"], dtype=float)
    turnover = np.asarray(current["turnover"], dtype=float)
    pe_ttm = np.asarray(current["daily_basic.pe_ttm"], dtype=float)
    is_st = np.asarray(current["stock_st.is_st"], dtype=float)
    suspended = np.asarray(current["suspended"], dtype=float)
    roe = np.asarray(fundamentals["fina_indicator.roe"], dtype=float)
    index_weight = mapped["weight"].to_numpy(dtype=float)
    industries = np.asarray(current["industry.l1_name"], dtype=object)

    valid = (
        (history_count >= MIN_HISTORY)
        & np.isfinite(momentum)
        & np.isfinite(volatility)
        & (volatility > 0.0)
        & np.isfinite(close)
        & (close > 0.0)
        & np.isfinite(volume)
        & (volume > 0.0)
        & np.isfinite(turnover)
        & (turnover >= MIN_DAILY_TURNOVER)
        & np.isfinite(pe_ttm)
        & (pe_ttm > 0.0)
        & np.isfinite(roe)
        & np.isfinite(index_weight)
        & (index_weight > 0.0)
        & (np.nan_to_num(is_st, nan=0.0) != 1.0)
        & (np.nan_to_num(suspended, nan=0.0) == 0.0)
    )

    frame = pd.DataFrame(
        {
            "asset": np.asarray(assets, dtype=object)[valid],
            "ts_code": np.asarray(codes, dtype=object)[valid],
            "industry": np.asarray(
                [
                    value if isinstance(value, str) and value else "未分类"
                    for value in industries
                ],
                dtype=object,
            )[valid],
            "index_weight": index_weight[valid],
            "momentum": momentum[valid],
            "volatility": volatility[valid],
            "pe_ttm": pe_ttm[valid],
            "roe": roe[valid],
        }
    )
    if frame.empty:
        return frame

    frame["momentum_rank"] = frame["momentum"].rank(pct=True)
    frame["quality_rank"] = frame["roe"].rank(pct=True)
    frame["value_rank"] = (-frame["pe_ttm"]).rank(pct=True)
    frame["low_vol_rank"] = (-frame["volatility"]).rank(pct=True)
    frame["score"] = (
        0.30 * frame["momentum_rank"]
        + 0.25 * frame["quality_rank"]
        + 0.25 * frame["value_rank"]
        + 0.20 * frame["low_vol_rank"]
    )
    return frame


def _industry_quotas(frame: pd.DataFrame, count: int) -> dict[str, int]:
    stats = frame.groupby("industry", sort=True).agg(
        capacity=("asset", "size"),
        index_weight=("index_weight", "sum"),
    )
    count = min(count, int(stats["capacity"].sum()))
    raw = count * stats["index_weight"] / stats["index_weight"].sum()
    quotas = np.floor(raw).astype(int).clip(upper=stats["capacity"])

    if count >= len(stats):
        quotas = quotas.where(quotas > 0, 1)
    while int(quotas.sum()) > count:
        choices = [name for name in stats.index if quotas[name] > 1]
        name = min(choices, key=lambda item: (raw[item] - quotas[item], str(item)))
        quotas[name] -= 1
    while int(quotas.sum()) < count:
        choices = [
            name for name in stats.index if quotas[name] < stats.at[name, "capacity"]
        ]
        name = max(
            choices,
            key=lambda item: (
                raw[item] - quotas[item],
                stats.at[item, "index_weight"],
                str(item),
            ),
        )
        quotas[name] += 1
    return {str(name): int(quota) for name, quota in quotas.items()}


def _select(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    quotas = _industry_quotas(frame, min(HOLD_COUNT, len(frame)))
    selected = []
    for industry, group in frame.groupby("industry", sort=True):
        ranked = group.sort_values(
            ["score", "index_weight", "ts_code"],
            ascending=[False, False, True],
        )
        selected.append(ranked.head(quotas[str(industry)]))
    return pd.concat(selected, ignore_index=True)


def _target_weights(
    candidates: pd.DataFrame, selected: pd.DataFrame
) -> dict[object, float]:
    sector_weights = candidates.groupby("industry")["index_weight"].sum()
    sector_weights /= sector_weights.sum()
    targets: dict[object, float] = {}
    for industry, group in selected.groupby("industry", sort=True):
        tilt = 1.0 + ACTIVE_TILT * (group["score"] - 0.5)
        raw = group["index_weight"] * tilt
        sector_target = TOTAL_EXPOSURE * float(sector_weights.loc[industry])
        normalized = sector_target * raw / raw.sum()
        targets.update(zip(group["asset"], normalized, strict=True))
    return targets


def _submit_targets(context, targets: dict[object, float]) -> int:
    portfolio_value = float(context.portfolio.portfolio_value)
    current_weights = {
        asset: float(position.amount * position.last_sale_price / portfolio_value)
        for asset, position in context.portfolio.positions.items()
        if position.amount > 0 and portfolio_value > 0
    }

    reductions: dict[object, float] = {}
    for asset, current_weight in current_weights.items():
        target = targets.get(asset, 0.0)
        if current_weight - target > WEIGHT_TOLERANCE:
            reductions[asset] = target

    increases = {
        asset: target
        for asset, target in targets.items()
        if target - current_weights.get(asset, 0.0) > WEIGHT_TOLERANCE
    }
    order_count = 0
    if reductions:
        order_count += len(order_target_percent_many(reductions))
    if increases:
        order_count += len(
            order_target_percent_many(increases, position_limit=HOLD_COUNT)
        )
    return order_count


def handle_data(context, data) -> None:
    month = (context.datetime.year, context.datetime.month)
    if month == context.last_rebalance_month:
        return
    if not context.fields_checked:
        _check_fields(data)
        context.fields_checked = True

    members = data.index_constituents(INDEX_CODE)
    if members.empty:
        record(universe_count=0, candidate_count=0, selected_count=0)
        return

    candidates = _factor_frame(data, members)
    selected = _select(candidates)
    if selected.empty:
        record(
            universe_count=len(members),
            candidate_count=len(candidates),
            selected_count=0,
        )
        return

    targets = _target_weights(candidates, selected)
    order_count = _submit_targets(context, targets)
    context.last_rebalance_month = month
    snapshot = pd.Timestamp(members["snapshot_date"].iloc[0]).strftime("%Y%m%d")
    record(
        universe_count=len(members),
        candidate_count=len(candidates),
        selected_count=len(selected),
        target_exposure=sum(targets.values()),
        submitted_orders=order_count,
        snapshot_date=int(snapshot),
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="运行国证 2000 指数增强策略和 TuAlpha 性能基准",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--start", default=START_DATE)
    parser.add_argument("--end", default=END_DATE)
    parser.add_argument("--capital-base", type=float, default=100_000_000.0)
    parser.add_argument("--bundle-root", default="~/.tualpha")
    parser.add_argument("--bundle-name", default="tualpha")
    parser.add_argument(
        "--output-dir", default="outputs/performance/guozheng2000_enhanced"
    )
    parser.add_argument("--column-cache-mib", type=int)
    parser.add_argument("--no-report", dest="generate_report", action="store_false")
    parser.add_argument("--no-progress", dest="show_progress", action="store_false")
    parser.set_defaults(generate_report=True, show_progress=True)
    return parser


def _write_benchmark(args, result, elapsed: float) -> Path:
    sessions = len(result.performance)
    order_status_counts = (
        {}
        if result.orders.empty
        else {
            str(name): int(count)
            for name, count in result.orders["status"].value_counts().items()
        }
    )
    reject_reason_counts = (
        {}
        if result.orders.empty
        else {
            str(name): int(count)
            for name, count in result.orders["reject_reason"]
            .dropna()
            .value_counts()
            .items()
            if str(name)
        }
    )
    rejected_orders = (
        0
        if result.orders.empty
        else int(result.orders["status"].isin(["rejected", "canceled"]).sum())
    )
    payload = {
        "strategy": "国证2000多因子指数增强",
        "tualpha_version": tualpha.__version__,
        "python_version": platform.python_version(),
        "start": args.start,
        "end": args.end,
        "capital_base": args.capital_base,
        "adjustment": "qfq",
        "execution_time": "open",
        "generate_report": args.generate_report,
        "sessions": sessions,
        "elapsed_seconds": round(elapsed, 6),
        "sessions_per_second": round(sessions / elapsed, 6) if elapsed else None,
        "orders": len(result.orders),
        "rejected_or_canceled_orders": rejected_orders,
        "order_status_counts": order_status_counts,
        "reject_reason_counts": reject_reason_counts,
        "transactions": len(result.transactions),
        "final_value": result.final_value,
    }
    destination = Path(args.output_dir) / "benchmark.json"
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return destination


def main() -> None:
    args = _parser().parse_args()
    started = perf_counter()
    result = run_algorithm(
        start=args.start,
        end=args.end,
        initialize=initialize,
        handle_data=handle_data,
        capital_base=args.capital_base,
        bundle_root=args.bundle_root,
        bundle_name=args.bundle_name,
        adjustment="qfq",
        execution_time="open",
        benchmark=INDEX_CODE,
        output_dir=args.output_dir,
        strategy_name="国证2000多因子指数增强",
        generate_report=args.generate_report,
        show_progress=args.show_progress,
        plotly_js="inline",
        column_cache_mib=args.column_cache_mib,
    )
    elapsed = perf_counter() - started
    benchmark_path = _write_benchmark(args, result, elapsed)
    print(result.summary().to_string())
    print(f"\n耗时：{elapsed:.3f} 秒")
    print(f"性能记录：{benchmark_path}")


if __name__ == "__main__":
    main()
