"""Point-in-time market-data API backed by DuckDB and Parquet."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

import numpy as np
import pandas as pd

from ..config import normalize_session
from ..model.asset import Asset


@dataclass(frozen=True, slots=True)
class DailyBar:
    """Raw exchange-price bar and A-share trading constraints."""

    asset: Asset
    session: pd.Timestamp
    open: float
    high: float
    low: float
    close: float
    pre_close: float
    volume: float
    turnover: float
    up_limit: float = np.nan
    down_limit: float = np.nan
    suspended: bool = False

    def value(self, field: str) -> float:
        name = "close" if field == "price" else field
        if not hasattr(self, name):
            raise KeyError(f"unknown daily bar field: {field}")
        return float(getattr(self, name))


from .portal import BundleDataPortal

TushareDataPortal = BundleDataPortal


class BarData:
    """A callback-scoped, point-in-time data facade similar to Zipline BarData."""

    def __init__(self, portal: BundleDataPortal) -> None:
        self._portal = portal
        self._session: pd.Timestamp | None = None

    @property
    def current_session(self) -> pd.Timestamp:
        if self._session is None:
            raise RuntimeError("BarData is not bound to a trading session")
        return self._session

    def _set_session(self, session: str | pd.Timestamp) -> None:
        self._session = normalize_session(session)

    @staticmethod
    def _asset_list(value: Asset | Iterable[Asset]) -> tuple[list[Asset], bool]:
        if isinstance(value, Asset):
            return [value], True
        return list(value), False

    @staticmethod
    def _field_list(value: str | Iterable[str]) -> tuple[list[str], bool]:
        if isinstance(value, str):
            return [value], True
        return list(value), False

    def current(
        self, assets: Asset | Iterable[Asset], fields: str | Iterable[str]
    ) -> object | pd.Series | pd.DataFrame:
        asset_list, one_asset = self._asset_list(assets)
        field_list, one_field = self._field_list(fields)
        if one_asset and one_field:
            return self._portal.value(
                asset_list[0],
                self.current_session,
                field_list[0],
                adjusted=True,
            )
        values = self._portal.values(
            asset_list,
            self.current_session,
            field_list,
            adjusted=True,
        )
        if one_asset:
            return pd.Series(
                {field: values[field][0] for field in field_list},
                name=asset_list[0].ts_code,
            )
        index = pd.Index([asset.ts_code for asset in asset_list], name=None)
        if one_field:
            return pd.Series(
                values[field_list[0]],
                index=index,
                name=field_list[0],
            )
        columns = {
            field: (
                pd.Series(array, index=index, dtype=object)
                if array.dtype == object
                else array
            )
            for field, array in values.items()
        }
        return pd.DataFrame(columns, index=index)

    def prefetch(
        self, assets: Asset | Iterable[Asset], fields: str | Iterable[str]
    ) -> None:
        """Warm repeated cross-sectional columns without exposing physical storage."""

        asset_list, _ = self._asset_list(assets)
        field_list, _ = self._field_list(fields)
        self._portal.prefetch(asset_list, field_list)

    def current_arrays(
        self, assets: Asset | Iterable[Asset], fields: str | Iterable[str]
    ) -> dict[str, np.ndarray]:
        """Return read-only NumPy arrays for a batch current-data query."""

        asset_list, _ = self._asset_list(assets)
        field_list, _ = self._field_list(fields)
        values = self._portal.values(
            asset_list,
            self.current_session,
            field_list,
            adjusted=True,
        )
        for array in values.values():
            array.setflags(write=False)
        return values

    def raw_current(self, asset: Asset, field: str) -> object:
        return self._portal.value(asset, self.current_session, field, adjusted=False)

    def history(
        self,
        assets: Asset | Iterable[Asset],
        fields: str | Iterable[str],
        bar_count: int,
    ) -> pd.Series | pd.DataFrame:
        asset_list, one_asset = self._asset_list(assets)
        field_list, one_field = self._field_list(fields)
        frame = self._portal.history(
            asset_list,
            field_list,
            self.current_session,
            bar_count,
            adjusted=True,
        )
        if one_asset and one_field:
            result = frame[(asset_list[0].ts_code, field_list[0])]
            result.name = asset_list[0].ts_code
            return result
        if one_field:
            return frame.xs(field_list[0], axis=1, level="field")
        if one_asset:
            return frame.xs(asset_list[0].ts_code, axis=1, level="asset")
        return frame

    def fundamental(
        self,
        asset: Asset,
        field: str,
        *,
        period: str | pd.Timestamp = "latest",
        report_type: str | None = "1",
    ) -> float:
        return self._portal.fundamental(
            asset,
            self.current_session,
            field,
            period=period,
            report_type=report_type,
        )

    def fundamentals(
        self,
        asset: Asset,
        fields: str | Iterable[str],
        *,
        periods: int = 4,
        report_type: str | None = "1",
    ) -> pd.DataFrame:
        field_list, _ = self._field_list(fields)
        return self._portal.fundamentals(
            asset,
            self.current_session,
            field_list,
            periods=periods,
            report_type=report_type,
        )

    def index_constituents(self, index_code: str) -> pd.DataFrame:
        """Return the latest index constituents visible before this session."""

        return self._portal.index_constituents(index_code, self.current_session)

    def available_fields(self, namespace: str | None = None) -> tuple[str, ...]:
        return self._portal.available_fields(namespace)

    def can_trade(self, asset: Asset) -> bool:
        return self._portal.is_tradable(asset, self.current_session)
