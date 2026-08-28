"""Cross-sectional factor diagnostics, neutralization, and report export."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from math import erfc, sqrt
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from ..data.research import FactorData
from ..foundation.config import DEFAULT_BUNDLE_ROOT, AdjustmentMode, PlotlyJsMode
from .result import _write_atomic

TRADING_DAYS_PER_YEAR = 252


def _row_correlation(
    left: pd.DataFrame,
    right: pd.DataFrame,
    minimum: int,
) -> tuple[pd.Series, pd.Series]:
    left, right = left.align(right, join="inner", axis=0)
    left, right = left.align(right, join="inner", axis=1)
    x = left.to_numpy(dtype=float)
    y = right.to_numpy(dtype=float)
    valid = ~np.isnan(x) & ~np.isnan(y)
    count = valid.sum(axis=1)
    x_valid = np.where(valid, x, 0.0)
    y_valid = np.where(valid, y, 0.0)
    with np.errstate(divide="ignore", invalid="ignore", over="ignore"):
        mean_x = x_valid.sum(axis=1) / count
        mean_y = y_valid.sum(axis=1) / count
        x_centered = np.where(valid, x - mean_x[:, None], 0.0)
        y_centered = np.where(valid, y - mean_y[:, None], 0.0)
        covariance = (x_centered * y_centered).sum(axis=1)
        variance_x = (x_centered * x_centered).sum(axis=1)
        variance_y = (y_centered * y_centered).sum(axis=1)
        denominator = np.sqrt(np.maximum(variance_x * variance_y, 0.0))
        eligible = (count >= minimum) & (denominator > 0.0)
        correlation_values = np.divide(
            covariance,
            denominator,
            out=np.full(len(left), np.nan, dtype=float),
            where=eligible,
        )
    return (
        pd.Series(correlation_values, index=left.index),
        pd.Series(count.astype(int), index=left.index),
    )


def _encode_industries(industries: pd.DataFrame) -> tuple[pd.DataFrame, np.ndarray]:
    values = industries.to_numpy(dtype=object)
    codes, names = pd.factorize(values.ravel(), sort=False)
    encoded = codes.reshape(values.shape).astype(np.int32, copy=False)
    encoded[np.asarray(values == "", dtype=bool)] = -1
    return (
        pd.DataFrame(encoded, index=industries.index, columns=industries.columns),
        np.asarray(names, dtype=object),
    )


def _neutralize_factor_values(
    factor: pd.DataFrame,
    *,
    industries: pd.DataFrame | None = None,
    market_caps: pd.DataFrame | None = None,
    industry_codes: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if industries is None and market_caps is None:
        return factor.astype(float).copy()
    values = factor.astype(float)
    encoded_industries = (
        industry_codes.reindex(
            index=values.index,
            columns=values.columns,
            fill_value=-1,
        )
        if industry_codes is not None
        else (
            _encode_industries(
                industries.reindex(index=values.index, columns=values.columns)
            )[0]
            if industries is not None
            else None
        )
    )
    industry_values = (
        encoded_industries.to_numpy(dtype=np.int32, copy=False)
        if encoded_industries is not None
        else None
    )
    cap_values = (
        market_caps.reindex(index=values.index, columns=values.columns).to_numpy(
            dtype=float
        )
        if market_caps is not None
        else None
    )
    source = values.to_numpy(dtype=float)
    output = np.full_like(source, np.nan, dtype=float)
    for row in range(len(values)):
        y = source[row]
        valid = np.isfinite(y)
        if industry_values is not None:
            industry_row = industry_values[row]
            valid &= industry_row >= 0
        if cap_values is not None:
            cap_row = cap_values[row]
            valid &= np.isfinite(cap_row) & (cap_row > 0.0)
        positions = np.flatnonzero(valid)
        if len(positions) < 2:
            continue
        residual = y[positions].copy()
        codes: np.ndarray | None = None
        if industry_values is not None:
            codes = industry_values[row, positions]
            counts = np.bincount(codes).astype(float)
            residual -= (np.bincount(codes, weights=residual) / counts)[codes]
        else:
            residual -= residual.mean()
        if cap_values is not None:
            size = np.log(cap_values[row, positions])
            if codes is not None:
                counts = np.bincount(codes).astype(float)
                size -= (np.bincount(codes, weights=size) / counts)[codes]
            else:
                size -= size.mean()
            denominator = float(np.dot(size, size))
            if denominator > np.finfo(float).eps:
                residual -= float(np.dot(size, residual) / denominator) * size
        output[row, positions] = residual
    return pd.DataFrame(output, index=values.index, columns=values.columns)


def neutralize_factor_values(
    factor: pd.DataFrame,
    *,
    industries: pd.DataFrame | None = None,
    market_caps: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Residualize daily factor values against industry and/or log market cap.

    Supplying both exposures is equivalent to a joint regression with industry
    fixed effects and ``log(total_mv)``. Missing exposures are excluded rather
    than filled, so they cannot enter later rankings or correlations.
    """

    return _neutralize_factor_values(
        factor,
        industries=industries,
        market_caps=market_caps,
    )


def _quantile_labels(factor: pd.DataFrame, quantiles: int) -> pd.DataFrame:
    ranked = factor.rank(axis=1, method="first", pct=True)
    return np.ceil(ranked * quantiles).clip(1, quantiles)


def _quantile_turnover(labels: pd.DataFrame, quantile: int) -> pd.Series:
    values = np.full(len(labels), np.nan, dtype=float)
    membership = labels.to_numpy(dtype=float) == quantile
    if len(labels) > 1:
        current_count = membership[1:].sum(axis=1)
        intersection = (membership[1:] & membership[:-1]).sum(axis=1)
        eligible = current_count > 0
        values[1:][eligible] = 1.0 - intersection[eligible] / current_count[eligible]
    return pd.Series(values, index=labels.index, dtype=float)


def _rank_autocorrelation(
    factor: pd.DataFrame,
    ranks: pd.DataFrame | None = None,
) -> pd.Series:
    ranks = factor.rank(axis=1, pct=True) if ranks is None else ranks
    correlation, _ = _row_correlation(ranks, ranks.shift(1), minimum=2)
    correlation.name = "factor_rank_autocorrelation"
    return correlation


def _quantile_statistics(
    returns: pd.DataFrame,
    labels: pd.DataFrame,
    quantiles: int,
) -> tuple[pd.DataFrame, pd.Series, pd.Series]:
    return_values = returns.to_numpy(dtype=float)
    label_values = labels.to_numpy(dtype=float)
    valid_returns = ~np.isnan(return_values)
    rows = []
    for quantile in range(1, quantiles + 1):
        selected = (label_values == quantile) & valid_returns
        counts = selected.sum(axis=1)
        sums = np.where(selected, return_values, 0.0).sum(axis=1)
        values = np.divide(
            sums,
            counts,
            out=np.full(len(returns), np.nan, dtype=float),
            where=counts > 0,
        )
        rows.append(
            pd.DataFrame(
                {
                    "date": returns.index,
                    "quantile": quantile,
                    "return": values,
                    "count": counts.astype(int),
                }
            )
        )
    means = np.stack([row["return"].to_numpy(dtype=float) for row in rows], axis=1)
    return (
        pd.concat(rows, ignore_index=True),
        pd.Series(means[:, -1], index=returns.index),
        pd.Series(means[:, 0], index=returns.index),
    )


def _factor_portfolio_returns(
    factor: pd.DataFrame,
    returns: pd.DataFrame,
) -> tuple[pd.Series, pd.Series]:
    factor_values = factor.to_numpy(dtype=float)
    return_values = returns.to_numpy(dtype=float)
    valid = ~np.isnan(factor_values) & ~np.isnan(return_values)
    count = valid.sum(axis=1)
    factor_valid = np.where(valid, factor_values, 0.0)
    return_valid = np.where(valid, return_values, 0.0)
    with np.errstate(divide="ignore", invalid="ignore", over="ignore"):
        means = np.divide(
            factor_valid.sum(axis=1),
            count,
            out=np.full(len(factor), np.nan, dtype=float),
            where=count > 0,
        )
        centered = np.where(valid, factor_values - means[:, None], 0.0)
        gross = np.abs(centered).sum(axis=1)
        factor_returns = np.divide(
            (centered * return_valid).sum(axis=1),
            gross,
            out=np.full(len(factor), np.nan, dtype=float),
            where=gross > 0.0,
        )
        universe_returns = np.divide(
            return_valid.sum(axis=1),
            count,
            out=np.full(len(factor), np.nan, dtype=float),
            where=count > 0,
        )
    return (
        pd.Series(factor_returns, index=factor.index),
        pd.Series(universe_returns, index=factor.index),
    )


def _sector_statistics(
    factor: pd.DataFrame,
    returns: pd.DataFrame,
    industry_codes: pd.DataFrame,
    industry_names: np.ndarray,
    labels: pd.DataFrame,
    *,
    factor_name: str,
    period: int,
    quantiles: int,
    minimum_observations: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    factor_values = factor.to_numpy(dtype=float)
    return_values = returns.to_numpy(dtype=float)
    industry_values = industry_codes.reindex(
        index=factor.index,
        columns=factor.columns,
        fill_value=-1,
    ).to_numpy(dtype=np.int32, copy=False)
    label_values = labels.to_numpy(dtype=float)
    minimum = max(3, min(int(minimum_observations), 5))
    row_count, column_count = factor_values.shape
    sector_count = len(industry_names)
    group_count = row_count * sector_count

    valid = (
        np.isfinite(factor_values) & np.isfinite(return_values) & (industry_values >= 0)
    )
    positions = np.flatnonzero(valid)
    rows = positions // column_count
    sectors = industry_values.ravel()[positions]
    keys = rows * sector_count + sectors
    x = factor_values.ravel()[positions]
    y = return_values.ravel()[positions]
    counts = np.bincount(keys, minlength=group_count).astype(float)
    sum_x = np.bincount(keys, weights=x, minlength=group_count)
    sum_y = np.bincount(keys, weights=y, minlength=group_count)
    sum_x2 = np.bincount(keys, weights=x * x, minlength=group_count)
    sum_y2 = np.bincount(keys, weights=y * y, minlength=group_count)
    sum_xy = np.bincount(keys, weights=x * y, minlength=group_count)
    with np.errstate(divide="ignore", invalid="ignore", over="ignore"):
        covariance = sum_xy - sum_x * sum_y / counts
        variance = (sum_x2 - sum_x * sum_x / counts) * (sum_y2 - sum_y * sum_y / counts)
        correlations = np.divide(
            covariance,
            np.sqrt(np.maximum(variance, 0.0)),
            out=np.full(group_count, np.nan, dtype=float),
            where=(counts >= minimum) & (variance > 0.0),
        ).reshape(row_count, sector_count)
    ic_observations = np.isfinite(correlations).sum(axis=0)
    ic_means = np.divide(
        np.nansum(correlations, axis=0),
        ic_observations,
        out=np.full(sector_count, np.nan, dtype=float),
        where=ic_observations > 0,
    )

    valid_quantile = (
        np.isfinite(return_values) & np.isfinite(label_values) & (industry_values >= 0)
    )
    positions = np.flatnonzero(valid_quantile)
    rows = positions // column_count
    sectors = industry_values.ravel()[positions]
    quantile_codes = label_values.ravel()[positions].astype(int) - 1
    keys = (rows * sector_count + sectors) * quantiles + quantile_codes
    quantile_group_count = group_count * quantiles
    quantile_counts = np.bincount(keys, minlength=quantile_group_count)
    quantile_sums = np.bincount(
        keys,
        weights=return_values.ravel()[positions],
        minlength=quantile_group_count,
    )
    daily_quantile_means = np.divide(
        quantile_sums,
        quantile_counts,
        out=np.full(quantile_group_count, np.nan, dtype=float),
        where=quantile_counts > 0,
    ).reshape(row_count, sector_count, quantiles)
    quantile_observations = np.isfinite(daily_quantile_means).sum(axis=0)
    quantile_means = np.divide(
        np.nansum(daily_quantile_means, axis=0),
        quantile_observations,
        out=np.full((sector_count, quantiles), np.nan, dtype=float),
        where=quantile_observations > 0,
    )

    sector_ic = pd.DataFrame(
        [
            {
                "factor": factor_name,
                "period": period,
                "sector": str(industry_names[sector]),
                "mean_ic": ic_means[sector],
                "observations": int(ic_observations[sector]),
            }
            for sector in np.flatnonzero(ic_observations)
        ]
    )
    sector_quantile = pd.DataFrame(
        [
            {
                "factor": factor_name,
                "period": period,
                "sector": str(industry_names[sector]),
                "quantile": quantile + 1,
                "mean_return": quantile_means[sector, quantile],
                "observations": int(quantile_observations[sector, quantile]),
            }
            for sector, quantile in zip(
                *np.nonzero(quantile_observations),
                strict=True,
            )
        ]
    )
    return sector_ic, sector_quantile


def _summary_metrics(daily: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for (factor, period), group in daily.groupby(["factor", "period"], sort=False):
        ic = group["ic"].dropna()
        rank_ic = group["rank_ic"].dropna()
        ic_std = float(ic.std(ddof=1)) if len(ic) > 1 else np.nan
        rank_std = float(rank_ic.std(ddof=1)) if len(rank_ic) > 1 else np.nan
        mean_ic = float(ic.mean()) if len(ic) else np.nan
        mean_rank = float(rank_ic.mean()) if len(rank_ic) else np.nan
        icir = mean_ic / ic_std if ic_std and np.isfinite(ic_std) else np.nan
        rank_icir = (
            mean_rank / rank_std if rank_std and np.isfinite(rank_std) else np.nan
        )
        ic_t_stat = (
            mean_ic / (ic_std / np.sqrt(len(ic)))
            if ic_std and np.isfinite(ic_std)
            else np.nan
        )
        rows.append(
            {
                "factor": factor,
                "period": int(period),
                "observations": len(ic),
                "mean_ic": mean_ic,
                "ic_std": ic_std,
                "icir": icir,
                "annualized_icir": icir * np.sqrt(TRADING_DAYS_PER_YEAR),
                "ic_positive_rate": float((ic > 0).mean()) if len(ic) else np.nan,
                "ic_t_stat": ic_t_stat,
                "ic_p_value": erfc(abs(ic_t_stat) / sqrt(2.0))
                if np.isfinite(ic_t_stat)
                else np.nan,
                "ic_skew": float(ic.skew()) if len(ic) > 2 else np.nan,
                "ic_kurtosis": float(ic.kurt()) if len(ic) > 3 else np.nan,
                "mean_rank_ic": mean_rank,
                "rank_ic_std": rank_std,
                "rank_icir": rank_icir,
                "rank_ic_positive_rate": float((rank_ic > 0).mean())
                if len(rank_ic)
                else np.nan,
                "mean_long_short_return": float(group["long_short_return"].mean()),
                "mean_top_quantile_turnover": float(
                    group["top_quantile_turnover"].mean()
                ),
                "mean_bottom_quantile_turnover": float(
                    group["bottom_quantile_turnover"].mean()
                ),
                "mean_factor_rank_autocorrelation": float(
                    group["factor_rank_autocorrelation"].mean()
                ),
                "mean_valid_count": float(group["valid_count"].mean()),
            }
        )
    return pd.DataFrame(rows)


def _dailyized_return(value: float, period: int) -> float:
    if not np.isfinite(value) or value <= -1.0:
        return np.nan
    return float(np.expm1(np.log1p(value) / period))


def _alpha_beta(
    factor_returns: pd.Series,
    universe_returns: pd.Series,
    period: int,
) -> tuple[float, float, float, int]:
    values = pd.concat([factor_returns, universe_returns], axis=1).dropna()
    if len(values) < 3:
        return np.nan, np.nan, np.nan, len(values)
    y = values.iloc[:, 0].to_numpy(dtype=float)
    x = values.iloc[:, 1].to_numpy(dtype=float)
    design = np.column_stack([np.ones(len(values)), x])
    coefficients = np.linalg.lstsq(design, y, rcond=None)[0]
    residuals = y - design @ coefficients
    degrees = len(values) - design.shape[1]
    covariance = (
        float(np.dot(residuals, residuals))
        / degrees
        * np.linalg.pinv(design.T @ design)
    )
    standard_error = float(np.sqrt(max(covariance[0, 0], 0.0)))
    alpha = float(coefficients[0])
    annual_alpha = (
        float(np.expm1(np.log1p(alpha) * TRADING_DAYS_PER_YEAR / period))
        if alpha > -1.0
        else np.nan
    )
    t_stat = alpha / standard_error if standard_error > 0.0 else np.nan
    return annual_alpha, t_stat, float(coefficients[1]), len(values)


def _return_analysis(
    daily: pd.DataFrame,
    quantile_returns: pd.DataFrame,
    quantiles: int,
) -> pd.DataFrame:
    rows = []
    for (factor, period), group in daily.groupby(["factor", "period"], sort=False):
        annual_alpha, alpha_t_stat, beta, observations = _alpha_beta(
            group.set_index("date")["factor_weighted_return"],
            group.set_index("date")["universe_return"],
            int(period),
        )
        quantile_group = quantile_returns[
            (quantile_returns["factor"] == factor)
            & (quantile_returns["period"] == period)
        ]
        top = float(
            quantile_group.loc[quantile_group["quantile"] == quantiles, "return"].mean()
        )
        bottom = float(
            quantile_group.loc[quantile_group["quantile"] == 1, "return"].mean()
        )
        top_daily = _dailyized_return(top, int(period))
        bottom_daily = _dailyized_return(bottom, int(period))
        rows.append(
            {
                "factor": factor,
                "period": int(period),
                "observations": observations,
                "annual_alpha": annual_alpha,
                "alpha_t_stat": alpha_t_stat,
                "beta": beta,
                "mean_daily_top_bps": top_daily * 10_000.0,
                "mean_daily_bottom_bps": bottom_daily * 10_000.0,
                "mean_daily_spread_bps": (top_daily - bottom_daily) * 10_000.0,
            }
        )
    return pd.DataFrame(rows)


@dataclass(slots=True)
class FactorAnalysisResult:
    """Structured factor diagnostics and atomic report export helpers."""

    factors: tuple[str, ...]
    periods: tuple[int, ...]
    start: pd.Timestamp
    end: pd.Timestamp
    daily_metrics: pd.DataFrame
    summary_metrics: pd.DataFrame
    quantile_returns: pd.DataFrame
    factor_expressions: dict[str, str] = field(default_factory=dict)
    return_analysis: pd.DataFrame = field(default_factory=pd.DataFrame)
    sector_ic: pd.DataFrame = field(default_factory=pd.DataFrame)
    sector_quantile_returns: pd.DataFrame = field(default_factory=pd.DataFrame)
    title: str = "TuAlpha 因子分析"
    plotly_js: PlotlyJsMode = PlotlyJsMode.INLINE
    metadata: dict[str, Any] = field(default_factory=dict)
    report_path: Path | None = None
    metrics_path: Path | None = None

    def export_metrics(self, path: str | Path) -> Path:
        destination = Path(path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        _write_atomic(
            destination,
            lambda temporary: self.daily_metrics.to_csv(
                temporary,
                index=False,
                encoding="utf-8-sig",
                float_format="%.10f",
            ),
        )
        self.metrics_path = destination
        return destination

    def export_report(self, path: str | Path) -> Path:
        from ..report.factor_html import generate_factor_report

        if len(self.factors) != 1:
            raise ValueError(
                "a factor report requires exactly one factor; export each factor "
                "to a separate directory"
            )
        destination = Path(path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        _write_atomic(
            destination,
            lambda temporary: generate_factor_report(self, temporary),
        )
        self.report_path = destination
        return destination

    def export(self, output_dir: str | Path) -> FactorAnalysisResult:
        if len(self.factors) != 1:
            raise ValueError(
                "a factor report requires exactly one factor; export each factor "
                "to a separate directory"
            )
        directory = Path(output_dir)
        directory.mkdir(parents=True, exist_ok=True)
        self.export_metrics(directory / "daily_factor_metrics.csv")
        self.export_report(directory / "factor_report.html")
        return self

    def summary(self) -> pd.DataFrame:
        return self.summary_metrics.copy()


def analyze_factor_data(
    factors: pd.DataFrame,
    forward_returns: Mapping[int, pd.DataFrame],
    *,
    industries: pd.DataFrame | None = None,
    market_caps: pd.DataFrame | None = None,
    industry_neutral: bool = False,
    market_cap_neutral: bool = False,
    factor_expressions: Mapping[str, str] | None = None,
    quantiles: int = 5,
    minimum_observations: int = 5,
    title: str = "TuAlpha 因子分析",
    plotly_js: PlotlyJsMode | str = PlotlyJsMode.INLINE,
    metadata: Mapping[str, Any] | None = None,
    output_dir: str | Path | None = None,
) -> FactorAnalysisResult:
    """Calculate Alphalens-style diagnostics for factor matrices."""

    if not isinstance(factors.columns, pd.MultiIndex) or factors.columns.nlevels != 2:
        raise ValueError("factors must have (asset, factor) MultiIndex columns")
    if quantiles < 2:
        raise ValueError("quantiles must be at least 2")
    if minimum_observations < 2:
        raise ValueError("minimum_observations must be at least 2")
    if industry_neutral and industries is None:
        raise ValueError("industries are required when industry_neutral=True")
    if market_cap_neutral and market_caps is None:
        raise ValueError("market_caps are required when market_cap_neutral=True")
    periods = tuple(sorted({int(period) for period in forward_returns}))
    if not periods or periods[0] <= 0:
        raise ValueError("forward return periods must be positive")
    factor_names = tuple(dict.fromkeys(factors.columns.get_level_values(1)))
    if not factor_names:
        raise ValueError("factors must contain at least one factor column")
    if output_dir is not None and len(factor_names) != 1:
        raise ValueError(
            "a factor report requires exactly one factor; analyze and export each "
            "factor separately"
        )

    daily_rows: list[pd.DataFrame] = []
    quantile_rows: list[pd.DataFrame] = []
    sector_ic_rows: list[pd.DataFrame] = []
    sector_quantile_rows: list[pd.DataFrame] = []
    industry_codes, industry_names = (
        _encode_industries(industries)
        if industries is not None
        else (None, np.asarray([], dtype=object))
    )
    for factor_name in factor_names:
        factor = factors.xs(factor_name, axis=1, level=1).astype(float)
        factor = _neutralize_factor_values(
            factor,
            industries=industries if industry_neutral else None,
            market_caps=market_caps if market_cap_neutral else None,
            industry_codes=industry_codes if industry_neutral else None,
        )
        labels = _quantile_labels(factor, quantiles)
        factor_ranks = factor.rank(axis=1, pct=True)
        top_turnover = _quantile_turnover(labels, quantiles)
        bottom_turnover = _quantile_turnover(labels, 1)
        rank_autocorrelation = _rank_autocorrelation(factor, factor_ranks)
        for period in periods:
            returns = forward_returns[period].astype(float)
            factor_aligned, returns_aligned = factor.align(
                returns, join="inner", axis=0
            )
            factor_aligned, returns_aligned = factor_aligned.align(
                returns_aligned, join="inner", axis=1
            )
            labels_aligned = labels.reindex(
                index=factor_aligned.index, columns=factor_aligned.columns
            )
            ic, count = _row_correlation(
                factor_aligned, returns_aligned, minimum_observations
            )
            rank_ic, _ = _row_correlation(
                factor_ranks.reindex(
                    index=factor_aligned.index,
                    columns=factor_aligned.columns,
                ),
                returns_aligned.rank(axis=1, pct=True),
                minimum_observations,
            )
            quantile, top_return, bottom_return = _quantile_statistics(
                returns_aligned,
                labels_aligned,
                quantiles,
            )
            factor_return, universe_return = _factor_portfolio_returns(
                factor_aligned,
                returns_aligned,
            )
            quantile.insert(1, "factor", factor_name)
            quantile.insert(2, "period", period)
            quantile_rows.append(quantile)
            daily_rows.append(
                pd.DataFrame(
                    {
                        "date": factor_aligned.index,
                        "factor": factor_name,
                        "period": period,
                        "ic": ic.to_numpy(dtype=float),
                        "rank_ic": rank_ic.to_numpy(dtype=float),
                        "valid_count": count.to_numpy(dtype=int),
                        "top_quantile_return": top_return.to_numpy(dtype=float),
                        "bottom_quantile_return": bottom_return.to_numpy(dtype=float),
                        "long_short_return": (top_return - bottom_return).to_numpy(
                            dtype=float
                        ),
                        "top_quantile_turnover": top_turnover.reindex(
                            factor_aligned.index
                        ).to_numpy(dtype=float),
                        "bottom_quantile_turnover": bottom_turnover.reindex(
                            factor_aligned.index
                        ).to_numpy(dtype=float),
                        "factor_rank_autocorrelation": rank_autocorrelation.reindex(
                            factor_aligned.index
                        ).to_numpy(dtype=float),
                        "factor_weighted_return": factor_return.to_numpy(dtype=float),
                        "universe_return": universe_return.to_numpy(dtype=float),
                    }
                )
            )
            if industries is not None:
                assert industry_codes is not None
                sector_ic, sector_quantile = _sector_statistics(
                    factor_aligned,
                    returns_aligned,
                    industry_codes,
                    industry_names,
                    labels_aligned,
                    factor_name=factor_name,
                    period=period,
                    quantiles=quantiles,
                    minimum_observations=minimum_observations,
                )
                if not sector_ic.empty:
                    sector_ic_rows.append(sector_ic)
                if not sector_quantile.empty:
                    sector_quantile_rows.append(sector_quantile)

    daily_metrics = pd.concat(daily_rows, ignore_index=True).sort_values(
        ["date", "factor", "period"], kind="stable"
    )
    quantile_returns = pd.concat(quantile_rows, ignore_index=True).sort_values(
        ["date", "factor", "period", "quantile"], kind="stable"
    )
    expressions = {
        name: str((factor_expressions or {}).get(name, name)) for name in factor_names
    }
    result_metadata = dict(metadata or {})
    result_metadata.update(
        {
            "industry_neutral": bool(industry_neutral),
            "market_cap_neutral": bool(market_cap_neutral),
        }
    )
    result = FactorAnalysisResult(
        factors=factor_names,
        periods=periods,
        start=pd.Timestamp(daily_metrics["date"].min()),
        end=pd.Timestamp(daily_metrics["date"].max()),
        daily_metrics=daily_metrics.reset_index(drop=True),
        summary_metrics=_summary_metrics(daily_metrics),
        quantile_returns=quantile_returns.reset_index(drop=True),
        factor_expressions=expressions,
        return_analysis=_return_analysis(daily_metrics, quantile_returns, quantiles),
        sector_ic=(
            pd.concat(sector_ic_rows, ignore_index=True)
            if sector_ic_rows
            else pd.DataFrame(
                columns=["factor", "period", "sector", "mean_ic", "observations"]
            )
        ),
        sector_quantile_returns=(
            pd.concat(sector_quantile_rows, ignore_index=True)
            if sector_quantile_rows
            else pd.DataFrame(
                columns=[
                    "factor",
                    "period",
                    "sector",
                    "quantile",
                    "mean_return",
                    "observations",
                ]
            )
        ),
        title=title,
        plotly_js=PlotlyJsMode(plotly_js),
        metadata=result_metadata,
    )
    if output_dir is not None:
        result.export(output_dir)
    return result


def run_factor_analysis(
    factors: str | Sequence[str] | Mapping[str, str],
    *,
    start: str | pd.Timestamp,
    end: str | pd.Timestamp,
    assets: Sequence[Any] | None = None,
    index_code: str | None = None,
    periods: Sequence[int] = (1, 5, 10),
    quantiles: int = 5,
    minimum_observations: int = 5,
    industry_neutral: bool = False,
    market_cap_neutral: bool = False,
    industry_field: str = "industry.l1_name",
    market_cap_field: str = "daily_basic.total_mv",
    bundle_root: str | Path = DEFAULT_BUNDLE_ROOT,
    bundle_name: str = "tualpha",
    adjustment: AdjustmentMode | str = AdjustmentMode.RAW,
    exclude_st: bool = True,
    min_listed_days: int = 365,
    exclude_suspended: bool = True,
    column_cache_mib: int | None = None,
    output_dir: str | Path | None = None,
    title: str = "TuAlpha 因子分析",
    plotly_js: PlotlyJsMode | str = PlotlyJsMode.INLINE,
) -> FactorAnalysisResult:
    """Load PIT factor data and produce a complete Alphalens-style analysis."""

    if isinstance(factors, str):
        labels = (factors,)
        expressions = (factors,)
    elif isinstance(factors, Mapping):
        labels = tuple(str(name) for name in factors)
        expressions = tuple(str(expression) for expression in factors.values())
    else:
        expressions = tuple(str(expression) for expression in factors)
        labels = expressions
    if not expressions:
        raise ValueError("at least one factor expression is required")
    if output_dir is not None and len(expressions) != 1:
        raise ValueError(
            "a factor report requires exactly one factor; call run_factor_analysis "
            "once per factor with a separate output directory"
        )
    horizons = tuple(
        sorted({_period for value in periods if (_period := int(value)) > 0})
    )
    if not horizons:
        raise ValueError("at least one positive forward-return period is required")
    with FactorData(
        start=start,
        end=end,
        assets=assets,
        index_code=index_code,
        bundle_root=bundle_root,
        bundle_name=bundle_name,
        adjustment=adjustment,
        exclude_st=exclude_st,
        min_listed_days=min_listed_days,
        exclude_suspended=exclude_suspended,
        column_cache_mib=column_cache_mib,
    ) as data:
        return_expressions = tuple(
            f"FUTURE_RETURNS($close,{period})" for period in horizons
        )
        requested = tuple(
            dict.fromkeys(
                [
                    *expressions,
                    industry_field,
                    *([market_cap_field] if market_cap_neutral else []),
                    *return_expressions,
                ]
            )
        )
        loaded = data.history_arrays(requested, allow_future=True)
        factor_values = pd.concat(
            {
                label: loaded[expression]
                for label, expression in zip(labels, expressions, strict=True)
            },
            axis=1,
        ).swaplevel(0, 1, axis=1)
        factor_values.columns.names = ["asset", "field"]
        industries = loaded[industry_field]
        market_caps = loaded[market_cap_field] if market_cap_neutral else None
        forward = {
            period: loaded[expression]
            for period, expression in zip(horizons, return_expressions, strict=True)
        }
        metadata = {
            "index_code": data.index_code,
            "index_name": data.index_name,
            "asset_count": len(data.assets),
            "adjustment": data.adjustment.value,
            "exclude_st": data.exclude_st,
            "min_listed_days": data.min_listed_days,
            "exclude_suspended": data.exclude_suspended,
            "industry_field": industry_field,
            "market_cap_field": market_cap_field,
        }
    return analyze_factor_data(
        factor_values,
        forward,
        industries=industries,
        market_caps=market_caps,
        industry_neutral=industry_neutral,
        market_cap_neutral=market_cap_neutral,
        factor_expressions=dict(zip(labels, expressions, strict=True)),
        quantiles=quantiles,
        minimum_observations=minimum_observations,
        title=title,
        plotly_js=plotly_js,
        metadata=metadata,
        output_dir=output_dir,
    )


__all__ = [
    "FactorAnalysisResult",
    "analyze_factor_data",
    "neutralize_factor_values",
    "run_factor_analysis",
]
