"""Plotly report rendering, charts, and attribution tables."""

from .factor_html import generate_factor_report
from .html import generate_html_report

__all__ = ["generate_factor_report", "generate_html_report"]
