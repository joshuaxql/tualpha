"""Tradable stock and ETF assets loaded from ``assets.pk``."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

import duckdb
import pandas as pd

from ..config import normalize_session
from ..data.bundle.manager import (
    BUNDLE_NAME,
    acquire_bundle_read_lock,
    latest_bundle_path,
    release_bundle_read_lock,
)
from ..data.bundle.parquet_store import CATALOG_FILE, load_manifest
from ..exceptions import DataError, SymbolNotFound


class AssetType(StrEnum):
    STOCK = "stock"
    ETF = "etf"


class Board(StrEnum):
    MAIN = "main"
    CHINEXT = "chinext"
    STAR = "star"
    BSE = "bse"
    ETF = "etf"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True, order=True)
class Asset:
    """A tradable A-share stock or exchange-traded fund."""

    sid: int
    ts_code: str
    symbol: str
    name: str
    asset_type: AssetType
    exchange: str
    board: Board
    list_date: pd.Timestamp | None = None
    delist_date: pd.Timestamp | None = None
    price_tick: float = 0.01
    settlement_days: int = 1

    def is_alive_on(self, session: str | pd.Timestamp) -> bool:
        if (
            isinstance(session, pd.Timestamp)
            and session.tzinfo is None
            and session.hour == 0
            and session.minute == 0
            and session.second == 0
            and session.microsecond == 0
            and session.nanosecond == 0
        ):
            date = session
        else:
            date = normalize_session(session)
        if self.list_date is not None and date < self.list_date:
            return False
        return self.delist_date is None or date <= self.delist_date

    @property
    def is_stock(self) -> bool:
        return self.asset_type is AssetType.STOCK

    @property
    def is_etf(self) -> bool:
        return self.asset_type is AssetType.ETF

    def __str__(self) -> str:
        return self.ts_code


def _asset_date(value: object) -> pd.Timestamp | None:
    try:
        numeric = int(value)
    except (TypeError, ValueError):
        return None
    if numeric <= 0:
        return None
    try:
        return pd.to_datetime(str(numeric), format="%Y%m%d").normalize()
    except ValueError as exc:
        raise DataError(f"assets.pk contains an invalid asset date: {value}") from exc


class AssetFinder:
    """Resolve stable tradable assets from the Bundle asset manifest."""

    def __init__(self, bundle_root: str | Path, bundle_name: str = BUNDLE_NAME) -> None:
        self.bundle_root = Path(bundle_root).expanduser()
        self.bundle_name = bundle_name
        lock_key, _ = acquire_bundle_read_lock(self.bundle_root, bundle_name)
        try:
            self.bundle_path = latest_bundle_path(self.bundle_root, bundle_name)
            manifest = load_manifest(self.bundle_path)
            self.bundle_generation = str(manifest["generation"])
            connection = duckdb.connect(
                str(self.bundle_path / CATALOG_FILE), read_only=True
            )
            try:
                frame = connection.execute(
                    "SELECT * FROM assets WHERE tradable = TRUE ORDER BY sid"
                ).fetchdf()
            finally:
                connection.close()
            rows = frame.to_dict("records")
        finally:
            release_bundle_read_lock(lock_key)
        if not rows:
            raise DataError(
                f"bundle contains no stock or ETF assets: {self.bundle_path}"
            )

        assets: list[Asset] = []
        for row in rows:
            try:
                asset_type = AssetType(str(row["asset_type"]))
            except (KeyError, ValueError) as exc:
                raise DataError(
                    f"unsupported bundled asset type: {row.get('asset_type')}"
                ) from exc
            try:
                board = Board(str(row.get("board", "unknown")))
            except ValueError:
                board = Board.UNKNOWN
            code = str(row["ts_code"]).upper()
            assets.append(
                Asset(
                    sid=int(row["sid"]),
                    ts_code=code,
                    symbol=str(row.get("symbol") or code.split(".")[0]),
                    name=str(row.get("name", "")),
                    asset_type=asset_type,
                    exchange=str(row.get("exchange", "")),
                    board=board,
                    list_date=_asset_date(row.get("list_date")),
                    delist_date=_asset_date(row.get("delist_date")),
                    price_tick=float(row.get("price_tick", 0.01)),
                    settlement_days=int(row.get("settlement_days", 1)),
                )
            )
        self._assets = tuple(sorted(assets, key=lambda asset: asset.sid))
        self._by_sid = {asset.sid: asset for asset in self._assets}
        self._by_code = {asset.ts_code: asset for asset in self._assets}
        self._by_symbol: dict[str, list[Asset]] = {}
        for asset in self._assets:
            self._by_symbol.setdefault(asset.symbol, []).append(asset)

    def retrieve_asset(
        self,
        code: int | str,
        as_of_date: str | pd.Timestamp | None = None,
    ) -> Asset:
        """Resolve a sid, Tushare code, or unambiguous six-digit symbol."""

        if isinstance(code, int):
            candidates = [self._by_sid[code]] if code in self._by_sid else []
        else:
            key = code.upper().strip()
            candidates = (
                [self._by_code[key]]
                if key in self._by_code
                else self._by_symbol.get(key, [])
            )
        if as_of_date is not None:
            candidates = [
                asset for asset in candidates if asset.is_alive_on(as_of_date)
            ]
        if len(candidates) != 1:
            suffix = (
                f" as of {normalize_session(as_of_date).date()}"
                if as_of_date is not None
                else ""
            )
            raise SymbolNotFound(
                f"unable to resolve unique stock/ETF symbol {code!r}{suffix}"
            )
        return candidates[0]

    def assets(self, asset_type: AssetType | None = None) -> tuple[Asset, ...]:
        if asset_type is None:
            return self._assets
        return tuple(asset for asset in self._assets if asset.asset_type is asset_type)

    def __iter__(self) -> Iterator[Asset]:
        return iter(self._assets)

    def __len__(self) -> int:
        return len(self._assets)
