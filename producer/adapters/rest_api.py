"""REST API source adapter with exponential backoff."""
from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from typing import Any

import requests

from .base import RawDataPoint

logger = logging.getLogger(__name__)

_BACKOFF_START = 1
_BACKOFF_CAP = 60


def _log_error(component: str, message: str, **extra: Any) -> None:
    record = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "component": component,
        "level": "ERROR",
        "message": message,
        **extra,
    }
    logger.error(json.dumps(record))


class RestApiAdapter:
    """Fetches data from a REST API endpoint.

    Implements exponential backoff (start 1 s, double, cap 60 s) on
    connectivity failures and emits structured JSON error logs.
    """

    def __init__(self, url: str, timeout: int = 10) -> None:
        self.url = url
        self.timeout = timeout
        self._backoff = _BACKOFF_START

    def fetch(self) -> list[RawDataPoint]:
        """Call the REST API and return parsed data points."""
        while True:
            try:
                response = requests.get(self.url, timeout=self.timeout)
                response.raise_for_status()
                self._backoff = _BACKOFF_START  # reset on success
                return self._parse(response.json())
            except (requests.ConnectionError, requests.Timeout) as exc:
                _log_error(
                    "RestApiAdapter",
                    f"Connectivity failure: {exc}",
                    url=self.url,
                    retry_in=self._backoff,
                )
                time.sleep(self._backoff)
                self._backoff = min(self._backoff * 2, _BACKOFF_CAP)
            except requests.HTTPError as exc:
                _log_error(
                    "RestApiAdapter",
                    f"HTTP error: {exc}",
                    url=self.url,
                    status_code=exc.response.status_code if exc.response else None,
                )
                raise

    def _parse(self, data: Any) -> list[RawDataPoint]:
        """Convert raw API response to RawDataPoint list."""
        if isinstance(data, list):
            return [
                RawDataPoint(source="rest_api", value=item, raw=item if isinstance(item, dict) else {"value": item})
                for item in data
            ]
        if isinstance(data, dict):
            return [RawDataPoint(source="rest_api", value=data, raw=data)]
        return [RawDataPoint(source="rest_api", value=data, raw={"value": data})]
