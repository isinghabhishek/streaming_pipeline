"""WebSocket source adapter with exponential backoff."""
from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from typing import Any

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


class WebSocketAdapter:
    """Connects to a WebSocket URL and collects messages.

    Implements exponential backoff (start 1 s, double, cap 60 s) on
    connectivity failures and emits structured JSON error logs.

    fetch() opens a short-lived connection, collects up to `max_messages`
    messages (or until the server closes), then returns them as RawDataPoints.
    """

    def __init__(self, url: str, max_messages: int = 10, recv_timeout: float = 5.0) -> None:
        self.url = url
        self.max_messages = max_messages
        self.recv_timeout = recv_timeout
        self._backoff = _BACKOFF_START

    def fetch(self) -> list[RawDataPoint]:
        """Connect to the WebSocket and return collected data points."""
        while True:
            try:
                messages = asyncio.run(self._collect())
                self._backoff = _BACKOFF_START  # reset on success
                return self._parse(messages)
            except Exception as exc:  # noqa: BLE001
                _log_error(
                    "WebSocketAdapter",
                    f"Connectivity failure: {exc}",
                    url=self.url,
                    retry_in=self._backoff,
                )
                time.sleep(self._backoff)
                self._backoff = min(self._backoff * 2, _BACKOFF_CAP)

    async def _collect(self) -> list[str]:
        """Open a WebSocket connection and collect messages."""
        import websockets  # lazy import — optional dependency

        messages: list[str] = []
        async with websockets.connect(self.url) as ws:
            for _ in range(self.max_messages):
                try:
                    msg = await asyncio.wait_for(ws.recv(), timeout=self.recv_timeout)
                    messages.append(msg)
                except asyncio.TimeoutError:
                    break
        return messages

    def _parse(self, messages: list[str]) -> list[RawDataPoint]:
        """Convert raw WebSocket messages to RawDataPoint list."""
        points: list[RawDataPoint] = []
        for msg in messages:
            try:
                data = json.loads(msg)
            except json.JSONDecodeError:
                data = msg
            raw = data if isinstance(data, dict) else {"value": data}
            points.append(RawDataPoint(source="websocket", value=data, raw=raw))
        return points
