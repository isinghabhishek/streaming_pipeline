"""Producer main entry point.

Reads configuration from environment variables, builds the appropriate
source adapter and Kafka publisher, then runs the publish loop.
"""
from __future__ import annotations

import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from producer.adapters import IoTSimulatorAdapter, RestApiAdapter, WebSocketAdapter
from producer.adapters.base import RawDataPoint, SourceAdapter
from producer.publisher import Event, KafkaPublisher

# ---------------------------------------------------------------------------
# Structured logger
# ---------------------------------------------------------------------------

_COMPONENT = "Producer"


def _log(level: str, message: str, event_id: str | None = None, **extra: Any) -> None:
    """Emit a structured JSON log line to stdout."""
    record: dict[str, Any] = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "component": _COMPONENT,
        "level": level,
        "message": message,
    }
    if event_id is not None:
        record["event_id"] = event_id
    record.update(extra)
    print(json.dumps(record), flush=True)


# ---------------------------------------------------------------------------
# Environment helpers
# ---------------------------------------------------------------------------

def _require_env(name: str) -> str:
    """Return the value of env var *name*, or exit(1) with a structured log."""
    value = os.environ.get(name)
    if not value:
        _log("ERROR", f"Required environment variable '{name}' is missing or empty")
        raise SystemExit(1)
    return value


# ---------------------------------------------------------------------------
# Adapter factory
# ---------------------------------------------------------------------------

def build_adapter(source_type: str, source_url: str) -> SourceAdapter:
    """Return the correct adapter instance for *source_type*."""
    if source_type == "rest_api":
        return RestApiAdapter(url=source_url)
    if source_type == "iot_simulator":
        return IoTSimulatorAdapter()
    if source_type == "websocket":
        return WebSocketAdapter(url=source_url)
    _log("ERROR", f"Unknown SOURCE_TYPE '{source_type}'; expected rest_api, iot_simulator, or websocket")
    raise SystemExit(1)


# ---------------------------------------------------------------------------
# Main publish loop
# ---------------------------------------------------------------------------

def run_loop(
    adapter: SourceAdapter,
    publisher: KafkaPublisher,
    topic: str,
    poll_interval_ms: int,
) -> None:
    """Fetch data points in a loop and publish each one as an Event."""
    sleep_s = poll_interval_ms / 1000.0
    while True:
        data_points: list[RawDataPoint] = adapter.fetch()
        for dp in data_points:
            event_id = str(uuid.uuid4())
            payload = {k: str(v) for k, v in dp.raw.items()}
            event = Event(
                event_id=event_id,
                source=dp.source,
                timestamp=datetime.now(timezone.utc).isoformat(),
                payload=payload,
            )
            publisher.publish(topic, event)
            _log("INFO", "Event published", event_id=event_id, topic=topic, source=dp.source)
        time.sleep(sleep_s)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    source_type = _require_env("SOURCE_TYPE")
    source_url = _require_env("SOURCE_URL")
    bootstrap_servers = _require_env("KAFKA_BOOTSTRAP_SERVERS")
    schema_registry_url = _require_env("SCHEMA_REGISTRY_URL")

    poll_interval_ms_str = os.environ.get("POLL_INTERVAL_MS", "1000")
    try:
        poll_interval_ms = int(poll_interval_ms_str)
    except ValueError:
        _log("ERROR", f"POLL_INTERVAL_MS must be an integer, got '{poll_interval_ms_str}'")
        raise SystemExit(1)

    _log("INFO", "Producer starting", source_type=source_type, topic=source_type)

    adapter = build_adapter(source_type, source_url)
    publisher = KafkaPublisher(
        bootstrap_servers=bootstrap_servers,
        schema_registry_url=schema_registry_url,
    )

    run_loop(adapter, publisher, topic=source_type, poll_interval_ms=poll_interval_ms)


if __name__ == "__main__":
    main()
