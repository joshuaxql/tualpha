"""Parquet Bundle protocol, readers, importers, and online updater."""

from __future__ import annotations

from typing import Any

__all__ = [
    "BUNDLE_NAME",
    "BUNDLE_SCHEMA_VERSION",
    "DataUpdater",
    "LockedBundleData",
    "UpdateOptions",
    "UpdateResult",
    "bundle_path",
    "latest_bundle_path",
    "load_bundle_data",
    "validate_parquet_bundle",
]


def __getattr__(name: str) -> Any:
    if name in {
        "BUNDLE_NAME",
        "BUNDLE_SCHEMA_VERSION",
        "LockedBundleData",
        "bundle_path",
        "latest_bundle_path",
        "load_bundle_data",
        "validate_parquet_bundle",
    }:
        from . import manager

        return getattr(manager, name)
    if name in {"DataUpdater", "UpdateOptions", "UpdateResult"}:
        from . import updater

        return getattr(updater, name)
    raise AttributeError(name)
