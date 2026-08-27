"""Compatibility facade for HTML report generation."""

from .report.attribution import attribution_rows as _attribution_rows
from .report.attribution import geometric_attribution as _geometric_attribution
from .report.attribution import geometric_link as _geometric_link
from .report.attribution import rejection_rows as _rejection_rows
from .report.charts import base_layout as _base_layout
from .report.charts import benchmark_chart as _benchmark_chart
from .report.charts import daily_distribution as _daily_distribution
from .report.charts import dashboard as _dashboard
from .report.charts import empty_figure as _empty_figure
from .report.charts import figure_html as _figure_html
from .report.charts import rolling_risk as _rolling_risk
from .report.charts import yearly_returns as _yearly_returns
from .report.formatting import COLORS as _COLORS
from .report.formatting import MONTHS as _MONTHS
from .report.formatting import REJECTION_LABELS as _REJECTION_LABELS
from .report.formatting import finite as _finite
from .report.formatting import metric_card as _metric_card
from .report.formatting import money as _money
from .report.formatting import number as _number
from .report.formatting import percent as _percent
from .report.html import generate_html_report

__all__ = [
    "_COLORS",
    "_MONTHS",
    "_REJECTION_LABELS",
    "_attribution_rows",
    "_base_layout",
    "_benchmark_chart",
    "_daily_distribution",
    "_dashboard",
    "_empty_figure",
    "_figure_html",
    "_finite",
    "_geometric_attribution",
    "_geometric_link",
    "_metric_card",
    "_money",
    "_number",
    "_percent",
    "_rejection_rows",
    "_rolling_risk",
    "_yearly_returns",
    "generate_html_report",
]
