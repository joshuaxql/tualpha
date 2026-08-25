"""Immutable HDF5/NumPy/Pickle Bundle primitives for TuAlpha 0.8."""

from __future__ import annotations

import hashlib
import io
import json
import pickle
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any, BinaryIO

import h5py
import numpy as np
import pandas as pd

from .exceptions import DataError

BUNDLE_PROTOCOL = "tualpha.bundle/0.8"
BUNDLE_SCHEMA_VERSION = 7
ASSETS_FILE = "assets.pk"
TRADE_DATES_FILE = "trade_dates.npy"
HDF5_FILES: dict[str, str] = {
    "daily.h5": "daily",
    "adj_factor.h5": "adj_factor",
    "daily_basic.h5": "daily_basic",
    "stk_limit.h5": "stk_limit",
    "finance.h5": "finance",
    "industry.h5": "industry",
    "stock_st.h5": "stock_st",
    "moneyflow.h5": "moneyflow",
    "index_weight.h5": "index_weight",
    "suspend_d.h5": "suspend_d",
}
REQUIRED_BUNDLE_FILES = frozenset({ASSETS_FILE, TRADE_DATES_FILE, *HDF5_FILES})


def date_to_int(value: object) -> int:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is not None:
        timestamp = timestamp.tz_convert("Asia/Shanghai").tz_localize(None)
    return int(timestamp.year * 10_000 + timestamp.month * 100 + timestamp.day)


def dates_to_int(values: Iterable[object]) -> np.ndarray:
    dates = (
        values
        if isinstance(values, pd.DatetimeIndex)
        else pd.DatetimeIndex(pd.to_datetime(list(values)))
    )
    if dates.tz is not None:
        dates = dates.tz_convert("Asia/Shanghai").tz_localize(None)
    days = dates.to_numpy(dtype="datetime64[D]")
    years = days.astype("datetime64[Y]")
    months = days.astype("datetime64[M]")
    return np.asarray(
        (years.astype(np.int64) + 1970) * 10_000
        + (months.astype(np.int64) % 12 + 1) * 100
        + (days - months).astype(np.int64)
        + 1,
        dtype="<i4",
    )


def ints_to_dates(values: np.ndarray) -> pd.DatetimeIndex:
    numeric = np.asarray(values)
    try:
        return pd.DatetimeIndex(
            pd.to_datetime(numeric.astype(str), format="%Y%m%d", errors="raise")
        )
    except (TypeError, ValueError) as exc:
        raise DataError("Bundle contains invalid YYYYMMDD dates") from exc


def sha256_file(path: str | Path, *, chunk_size: int = 8 * 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        while chunk := stream.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def initialize_h5(
    path: str | Path,
    *,
    role: str,
    generation: str,
    generated_at: str,
) -> h5py.File:
    destination = Path(path)
    destination.unlink(missing_ok=True)
    handle = h5py.File(destination, "w", libver="latest")
    handle.attrs.update(
        {
            "protocol": BUNDLE_PROTOCOL,
            "schema_version": np.uint32(BUNDLE_SCHEMA_VERSION),
            "file_role": role,
            "generation": generation,
            "generated_at": generated_at,
            "date_encoding": "YYYYMMDD",
            "key_layout": "data/<ts_code>",
        }
    )
    handle.create_group("data", track_order=True)
    return handle


def create_compound_dataset(
    group: h5py.Group,
    name: str,
    values: np.ndarray,
    *,
    chunk_rows: int = 4096,
) -> h5py.Dataset:
    array = np.asarray(values)
    if array.ndim != 1 or array.dtype.names is None:
        raise TypeError("HDF5 Bundle datasets must be one-dimensional compound arrays")
    if name in group:
        del group[name]
    del chunk_rows
    return group.create_dataset(name, data=array)


class _RestrictedUnpickler(pickle.Unpickler):
    def find_class(self, module: str, name: str) -> Any:
        raise pickle.UnpicklingError(
            f"global objects are forbidden in assets.pk: {module}.{name}"
        )


def restricted_pickle_load(source: str | Path | BinaryIO) -> Any:
    if hasattr(source, "read"):
        return _RestrictedUnpickler(source).load()  # type: ignore[arg-type]
    with Path(source).open("rb") as stream:
        return _RestrictedUnpickler(stream).load()


def restricted_pickle_loads(payload: bytes) -> Any:
    return _RestrictedUnpickler(io.BytesIO(payload)).load()


def write_assets_manifest(path: str | Path, manifest: Mapping[str, Any]) -> None:
    payload = pickle.dumps(dict(manifest), protocol=5)
    restricted_pickle_loads(payload)
    destination = Path(path)
    temporary = destination.with_name(f".{destination.name}.tmp")
    temporary.write_bytes(payload)
    temporary.replace(destination)


def load_assets_manifest(path: str | Path) -> dict[str, Any]:
    source = Path(path)
    if not source.is_file():
        raise DataError(f"Bundle asset manifest does not exist: {source}")
    try:
        payload = restricted_pickle_load(source)
    except (OSError, EOFError, pickle.UnpicklingError, ValueError) as exc:
        raise DataError(f"assets.pk is invalid: {source}") from exc
    if not isinstance(payload, dict):
        raise DataError("assets.pk root must be a dictionary")
    if payload.get("protocol") != BUNDLE_PROTOCOL:
        raise DataError("assets.pk protocol is unsupported")
    if int(payload.get("schema_version", -1)) != BUNDLE_SCHEMA_VERSION:
        raise DataError("assets.pk schema version is unsupported")
    generation = payload.get("generation")
    if not isinstance(generation, str) or not generation:
        raise DataError("assets.pk generation is missing")
    assets = payload.get("assets")
    if not isinstance(assets, list) or not assets:
        raise DataError("assets.pk contains no assets")
    return payload


def load_trade_dates(path: str | Path) -> np.ndarray:
    source = Path(path)
    try:
        values = np.load(source, allow_pickle=False)
    except (OSError, ValueError) as exc:
        raise DataError(f"trade_dates.npy is invalid: {source}") from exc
    if values.ndim != 1 or values.dtype.kind not in {"i", "u"}:
        raise DataError("trade_dates.npy must be a one-dimensional integer array")
    dates = np.asarray(values, dtype="<i4")
    if not len(dates):
        raise DataError("trade_dates.npy contains no sessions")
    if np.any(dates[1:] <= dates[:-1]):
        raise DataError("trade_dates.npy must be strictly increasing and unique")
    ints_to_dates(dates)
    return dates


def open_h5_checked(
    path: str | Path,
    *,
    expected_role: str,
    expected_generation: str,
) -> h5py.File:
    source = Path(path)
    try:
        handle = h5py.File(source, "r", libver="latest", swmr=True)
    except (OSError, ValueError) as exc:
        raise DataError(f"HDF5 Bundle file is invalid: {source}") from exc
    try:
        if str(handle.attrs.get("protocol", "")) != BUNDLE_PROTOCOL:
            raise DataError(f"HDF5 protocol is invalid: {source.name}")
        if int(handle.attrs.get("schema_version", -1)) != BUNDLE_SCHEMA_VERSION:
            raise DataError(f"HDF5 schema version is invalid: {source.name}")
        if str(handle.attrs.get("file_role", "")) != expected_role:
            raise DataError(f"HDF5 role is invalid: {source.name}")
        if str(handle.attrs.get("generation", "")) != expected_generation:
            raise DataError(
                f"Bundle components come from different generations: {source.name}"
            )
        if "data" not in handle or not isinstance(handle["data"], h5py.Group):
            raise DataError(f"HDF5 data group is missing: {source.name}")
    except Exception:
        handle.close()
        raise
    return handle


def _validate_asset_rows(assets: Sequence[object]) -> None:
    sids: set[int] = set()
    codes: set[str] = set()
    for raw in assets:
        if not isinstance(raw, dict):
            raise DataError("assets.pk asset entries must be dictionaries")
        try:
            sid = int(raw["sid"])
            code = str(raw["ts_code"]).upper()
            asset_type = str(raw["asset_type"])
        except (KeyError, TypeError, ValueError) as exc:
            raise DataError("assets.pk contains an invalid asset entry") from exc
        if sid <= 0 or sid in sids or not code or code in codes:
            raise DataError("assets.pk contains duplicate or invalid asset identifiers")
        if asset_type not in {"stock", "etf", "index"}:
            raise DataError(f"unsupported bundled asset type: {asset_type}")
        sids.add(sid)
        codes.add(code)


def validate_bundle_directory(
    bundle_path: str | Path,
    *,
    full_hash: bool = False,
) -> dict[str, Any]:
    root = Path(bundle_path)
    if not root.is_dir():
        raise DataError(f"Bundle directory does not exist: {root}")
    actual = {path.name for path in root.iterdir() if path.is_file()}
    if actual != REQUIRED_BUNDLE_FILES:
        missing = sorted(REQUIRED_BUNDLE_FILES.difference(actual))
        extra = sorted(actual.difference(REQUIRED_BUNDLE_FILES))
        raise DataError(f"Bundle file set is invalid; missing={missing}, extra={extra}")
    if any(path.is_dir() for path in root.iterdir()):
        raise DataError("Bundle directory must contain only the 12 protocol files")

    manifest = load_assets_manifest(root / ASSETS_FILE)
    _validate_asset_rows(manifest["assets"])
    generation = str(manifest["generation"])
    sessions = load_trade_dates(root / TRADE_DATES_FILE)
    if int(manifest.get("session_count", -1)) != len(sessions):
        raise DataError("trade date count does not match assets.pk")
    if int(manifest.get("start_session", 0)) != int(sessions[0]):
        raise DataError("trade date start does not match assets.pk")
    if int(manifest.get("end_session", 0)) != int(sessions[-1]):
        raise DataError("trade date end does not match assets.pk")

    file_metadata = manifest.get("files")
    if not isinstance(file_metadata, dict):
        raise DataError("assets.pk file metadata is missing")
    for filename in sorted(REQUIRED_BUNDLE_FILES.difference({ASSETS_FILE})):
        metadata = file_metadata.get(filename)
        if not isinstance(metadata, dict):
            raise DataError(f"assets.pk has no metadata for {filename}")
        path = root / filename
        if int(metadata.get("size", -1)) != path.stat().st_size:
            raise DataError(f"Bundle file size does not match assets.pk: {filename}")
        if full_hash and str(metadata.get("sha256", "")) != sha256_file(path):
            raise DataError(f"Bundle file hash does not match assets.pk: {filename}")

    handles: list[h5py.File] = []
    try:
        for filename, role in HDF5_FILES.items():
            handles.append(
                open_h5_checked(
                    root / filename,
                    expected_role=role,
                    expected_generation=generation,
                )
            )
    finally:
        for handle in handles:
            handle.close()
    return manifest


def categories_from_h5(handle: h5py.File) -> dict[str, list[str]]:
    raw = handle.attrs.get("categories", "{}")
    try:
        payload = json.loads(str(raw))
    except ValueError as exc:
        raise DataError(f"invalid categorical dictionary in {handle.filename}") from exc
    if not isinstance(payload, dict):
        raise DataError(f"invalid categorical dictionary in {handle.filename}")
    return {str(key): [str(item) for item in value] for key, value in payload.items()}
