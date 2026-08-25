"""Fixed schema-7 HDF5 Bundle public API."""

from __future__ import annotations

from ._bundle_core import (
    BUNDLE_NAME,
    BundleBuildResult,
    acquire_bundle_read_lock,
    paths_overlap,
    release_bundle_read_lock,
)
from ._bundle_v8 import (
    BUNDLE_SCHEMA_VERSION,
    UPDATE_STATUS_SCHEMA_VERSION,
    LockedBundleData,
    build_bundle,
    bundle_lock_path,
    bundle_parent,
    bundle_path,
    cleanup_legacy_storage,
    latest_bundle_path,
    load_bundle_data,
    update_status_path,
    validate_bundle_name,
    validate_hdf5_bundle,
)
from ._hdf5_store import REQUIRED_BUNDLE_FILES

REQUIRED_BUNDLE_ENTRIES = tuple(sorted(REQUIRED_BUNDLE_FILES))
_validate_tualpha_files = validate_hdf5_bundle

__all__ = [
    "BUNDLE_NAME",
    "BUNDLE_SCHEMA_VERSION",
    "REQUIRED_BUNDLE_ENTRIES",
    "UPDATE_STATUS_SCHEMA_VERSION",
    "BundleBuildResult",
    "LockedBundleData",
    "acquire_bundle_read_lock",
    "build_bundle",
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
]
