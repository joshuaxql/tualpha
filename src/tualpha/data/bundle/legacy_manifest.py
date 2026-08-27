"""Minimal safe reader used only to preserve metadata from a legacy HDF5 Bundle."""

from __future__ import annotations

import pickle
from pathlib import Path
from typing import Any

from ...foundation.exceptions import DataError


class _RestrictedUnpickler(pickle.Unpickler):
    def find_class(self, module: str, name: str) -> Any:
        raise pickle.UnpicklingError(
            f"global objects are forbidden in legacy assets.pk: {module}.{name}"
        )


def load_legacy_assets_manifest(path: str | Path) -> dict[str, Any]:
    source = Path(path)
    try:
        with source.open("rb") as stream:
            payload = _RestrictedUnpickler(stream).load()
    except (OSError, EOFError, pickle.UnpicklingError, ValueError) as exc:
        raise DataError(f"legacy assets.pk is invalid: {source}") from exc
    if not isinstance(payload, dict) or not isinstance(payload.get("assets"), list):
        raise DataError(f"legacy assets.pk has an invalid structure: {source}")
    return payload
