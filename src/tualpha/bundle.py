"""Compatibility facade for the immutable Parquet Bundle runtime API."""

from __future__ import annotations

from .data.bundle.manager import (
    BUNDLE_NAME,
    BUNDLE_SCHEMA_VERSION,
    UPDATE_STATUS_SCHEMA_VERSION,
    LockedBundleData,
    acquire_bundle_read_lock,
    bundle_lock_path,
    bundle_parent,
    bundle_path,
    cleanup_legacy_storage,
    latest_bundle_path,
    load_bundle_data,
    paths_overlap,
    release_bundle_read_lock,
    update_status_path,
    validate_bundle_name,
    validate_hdf5_bundle,
    validate_parquet_bundle,
)
from .data.bundle.parquet_store import CATALOG_FILE, MANIFEST_FILE, PARQUET_DIRECTORY
from .data.bundle.registry import BundleBuildResult

REQUIRED_BUNDLE_ENTRIES = (CATALOG_FILE, MANIFEST_FILE, PARQUET_DIRECTORY)
_validate_tualpha_files = validate_parquet_bundle

__all__ = [
    "BUNDLE_NAME",
    "BUNDLE_SCHEMA_VERSION",
    "REQUIRED_BUNDLE_ENTRIES",
    "UPDATE_STATUS_SCHEMA_VERSION",
    "BundleBuildResult",
    "LockedBundleData",
    "acquire_bundle_read_lock",
    "bundle_lock_path",
    "bundle_parent",
    "bundle_path",
    "cleanup_legacy_storage",
    "latest_bundle_path",
    "load_bundle_data",
    "paths_overlap",
    "release_bundle_read_lock",
    "update_status_path",
    "validate_bundle_name",
    "validate_hdf5_bundle",
    "validate_parquet_bundle",
]
