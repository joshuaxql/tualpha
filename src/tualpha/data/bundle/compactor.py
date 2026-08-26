"""Compatibility no-op for the former HDF5 Delta compaction command."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from .manager import BUNDLE_NAME, latest_bundle_path
from .parquet_store import validate_bundle
from .registry import BundleBuildResult


def compact_bundle(
    bundle_root: str | Path,
    bundle_name: str = BUNDLE_NAME,
) -> tuple[BundleBuildResult, dict[str, object]]:
    """Validate and return the active Bundle; yearly Parquet needs no compaction."""

    path = latest_bundle_path(bundle_root, bundle_name)
    manifest = validate_bundle(path, full_hash=True)
    result = BundleBuildResult(
        path=path,
        start_session=pd.Timestamp(str(manifest["start_session"])),
        end_session=pd.Timestamp(str(manifest["end_session"])),
        asset_count=int(manifest["asset_count"]),
        session_count=int(manifest["session_count"]),
    )
    return result, manifest
