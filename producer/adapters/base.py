"""Base types for source adapters."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol


@dataclass
class RawDataPoint:
    """A single raw data point fetched from a source."""
    source: str
    value: Any
    raw: dict = field(default_factory=dict)


class SourceAdapter(Protocol):
    """Protocol that all source adapters must satisfy."""

    def fetch(self) -> list[RawDataPoint]:
        """Fetch data points from the source. Returns a list of RawDataPoint."""
        ...
