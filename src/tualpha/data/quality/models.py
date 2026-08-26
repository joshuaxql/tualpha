from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any


class Severity(StrEnum):
    WARN = "warn"
    FAIL = "fail"


@dataclass(frozen=True, slots=True)
class QualityFinding:
    table: str
    severity: Severity
    rule: str
    count: int
    message: str
    sample: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class TableSummary:
    table: str
    rows: int
    partitions: int
    start_date: str | None
    end_date: str | None
    fail_count: int
    warn_count: int

    @property
    def status(self) -> str:
        return "fail" if self.fail_count else "warn" if self.warn_count else "pass"

    def to_dict(self) -> dict[str, Any]:
        return {**asdict(self), "status": self.status}


@dataclass(slots=True)
class QualityReport:
    generation: str
    created_at: str
    summaries: list[TableSummary]
    findings: list[QualityFinding]
    metrics: list[dict[str, Any]]
    output_dir: Path | None = None

    @property
    def fail_count(self) -> int:
        return sum(1 for finding in self.findings if finding.severity is Severity.FAIL)

    @property
    def warn_count(self) -> int:
        return sum(1 for finding in self.findings if finding.severity is Severity.WARN)
