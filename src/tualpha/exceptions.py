"""Compatibility facade for the public exception hierarchy."""

from .foundation.exceptions import (
    ConfigurationError,
    DataError,
    NoActiveAlgorithm,
    SymbolNotFound,
    TualphaError,
)

__all__ = [
    "ConfigurationError",
    "DataError",
    "NoActiveAlgorithm",
    "SymbolNotFound",
    "TualphaError",
]
