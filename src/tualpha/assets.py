"""Stock and ETF assets loaded from the official Bundle asset database."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

import pandas as pd
from zipline.assets import ASSET_DB_VERSION

from .bundle import (
    BUNDLE_NAME,
    acquire_bundle_read_lock,
    latest_bundle_path,
    release_bundle_read_lock,
)
from .config import normalize_session
from .exceptions import DataError, SymbolNotFound


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


class AssetFinder:
    """Resolve stable assets from Zipline's asset SQLite database."""

    def __init__(self, bundle_root: str | Path, bundle_name: str = BUNDLE_NAME) -> None:
        self.bundle_root = Path(bundle_root).expanduser()
        self.bundle_name = bundle_name
        lock_key, _ = acquire_bundle_read_lock(self.bundle_root, bundle_name)
        try:
            self.bundle_path = latest_bundle_path(self.bundle_root, bundle_name)
            manifest = json.loads(
                (self.bundle_path / "manifest.json").read_text(encoding="utf-8")
            )
            self.bundle_generation = str(manifest["generated_at"])
            asset_db = self.bundle_path / f"assets-{ASSET_DB_VERSION}.sqlite"
            if not asset_db.is_file():
                raise DataError(f"bundle asset database does not exist: {asset_db}")
            uri = f"file:{asset_db.resolve().as_posix()}?mode=ro"
            connection = sqlite3.connect(uri, uri=True)
            try:
                rows = connection.execute(
                    """
                    WITH attributes AS (
                        SELECT sid,
                               max(CASE WHEN field = 'asset_type' THEN value END)
                                   AS asset_type,
                               max(CASE WHEN field = 'board' THEN value END) AS board,
                               max(CASE WHEN field = 'price_tick' THEN value END)
                                   AS price_tick
                        FROM equity_supplementary_mappings
                        GROUP BY sid
                    )
                    SELECT e.sid, m.symbol, e.asset_name, e.start_date, e.end_date,
                           e.exchange, a.asset_type, a.board, a.price_tick
                    FROM equities e
                    JOIN equity_symbol_mappings m ON m.sid = e.sid
                    LEFT JOIN attributes a ON a.sid = e.sid
                    ORDER BY e.sid, m.end_date DESC
                    """
                ).fetchall()
            finally:
                connection.close()
        finally:
            release_bundle_read_lock(lock_key)
        if not rows:
            raise DataError(f"bundle contains no stock or ETF assets: {asset_db}")

        assets = []
        seen: set[int] = set()
        for row in rows:
            sid = int(row[0])
            if sid in seen:
                continue
            seen.add(sid)
            try:
                asset_type = AssetType(str(row[6]))
            except ValueError as exc:
                raise DataError(f"unsupported bundled asset type: {row[6]}") from exc
            try:
                board = Board(str(row[7]))
            except ValueError:
                board = Board.UNKNOWN
            code = str(row[1]).upper()
            assets.append(
                Asset(
                    sid=sid,
                    ts_code=code,
                    symbol=code.split(".")[0],
                    name=str(row[2] or ""),
                    asset_type=asset_type,
                    exchange=str(row[5]),
                    board=board,
                    list_date=pd.Timestamp(int(row[3]), unit="ns").normalize(),
                    delist_date=pd.Timestamp(int(row[4]), unit="ns").normalize(),
                    price_tick=float(row[8]),
                    settlement_days=1,
                )
            )
        self._assets = tuple(assets)
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
