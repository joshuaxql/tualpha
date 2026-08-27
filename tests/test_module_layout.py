from __future__ import annotations


def test_legacy_module_facades_reexport_canonical_implementations() -> None:
    from tualpha.analysis.metrics import calculate_metrics as canonical_metrics
    from tualpha.analysis.result import BacktestResult as CanonicalResult
    from tualpha.config import BacktestConfig as LegacyConfig
    from tualpha.exceptions import DataError as LegacyDataError
    from tualpha.foundation.config import BacktestConfig as CanonicalConfig
    from tualpha.foundation.exceptions import DataError as CanonicalDataError
    from tualpha.metrics import calculate_metrics as legacy_metrics
    from tualpha.report.html import generate_html_report as canonical_report
    from tualpha.reporting import generate_html_report as legacy_report
    from tualpha.result import BacktestResult as LegacyResult

    assert LegacyConfig is CanonicalConfig
    assert LegacyDataError is CanonicalDataError
    assert legacy_metrics is canonical_metrics
    assert LegacyResult is CanonicalResult
    assert legacy_report is canonical_report


def test_reporting_private_compatibility_names_remain_available() -> None:
    from tualpha.report.attribution import (
        attribution_rows,
        geometric_attribution,
        geometric_link,
        rejection_rows,
    )
    from tualpha.reporting import (
        _attribution_rows,
        _geometric_attribution,
        _geometric_link,
        _rejection_rows,
    )

    assert _attribution_rows is attribution_rows
    assert _geometric_attribution is geometric_attribution
    assert _geometric_link is geometric_link
    assert _rejection_rows is rejection_rows
