"""Exception hierarchy shared by all TuAlpha modules."""


class TualphaError(Exception):
    """Base class for all framework-specific errors."""


class ConfigurationError(TualphaError, ValueError):
    """Raised when a backtest configuration is invalid."""


class DataError(TualphaError):
    """Raised when local market data is missing or malformed."""


class SymbolNotFound(TualphaError, LookupError):
    """Raised when a stock or ETF cannot be resolved."""


class NoActiveAlgorithm(TualphaError, RuntimeError):
    """Raised when a Zipline-style API is called outside a callback."""
