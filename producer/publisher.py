"""KafkaPublisher: serialize events to Avro via Schema Registry and produce to Kafka."""
from __future__ import annotations

import io
import json
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import fastavro

# ---------------------------------------------------------------------------
# Structured JSON logger
# ---------------------------------------------------------------------------

class _StructuredLogger:
    """Emits structured JSON log lines."""

    def __init__(self, component: str) -> None:
        self._component = component
        self._logger = logging.getLogger(component)

    def _emit(self, level: str, message: str, event_id: str | None = None, **extra: Any) -> None:
        record: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "component": self._component,
            "level": level,
            "message": message,
        }
        if event_id is not None:
            record["event_id"] = event_id
        record.update(extra)
        log_fn = getattr(self._logger, level.lower(), self._logger.info)
        log_fn(json.dumps(record))

    def info(self, message: str, event_id: str | None = None, **extra: Any) -> None:
        self._emit("INFO", message, event_id, **extra)

    def warning(self, message: str, event_id: str | None = None, **extra: Any) -> None:
        self._emit("WARNING", message, event_id, **extra)

    def error(self, message: str, event_id: str | None = None, **extra: Any) -> None:
        self._emit("ERROR", message, event_id, **extra)


_log = _StructuredLogger("KafkaPublisher")

# ---------------------------------------------------------------------------
# Schema loading helpers
# ---------------------------------------------------------------------------

_SCHEMAS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "schemas")


def _load_schema(filename: str) -> dict:
    path = os.path.join(_SCHEMAS_DIR, filename)
    with open(path) as f:
        return fastavro.parse_schema(json.load(f))


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

@dataclass
class Event:
    event_id: str
    source: str
    timestamp: str
    payload: dict[str, str] = field(default_factory=dict)


@dataclass
class DLQEvent:
    original_topic: str
    original_partition: int
    original_offset: int
    error_type: str
    error_message: str
    raw_bytes: bytes


# ---------------------------------------------------------------------------
# Avro serialization helpers (using fastavro; no live Schema Registry needed)
# ---------------------------------------------------------------------------

def _serialize_event(event: Event, schema: dict) -> bytes:
    record = {
        "event_id": event.event_id,
        "source": event.source,
        "timestamp": event.timestamp,
        "payload": event.payload,
    }
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, schema, record)
    return buf.getvalue()


def _serialize_dlq_event(dlq_event: DLQEvent, schema: dict) -> bytes:
    record = {
        "original_topic": dlq_event.original_topic,
        "original_partition": dlq_event.original_partition,
        "original_offset": dlq_event.original_offset,
        "error_type": dlq_event.error_type,
        "error_message": dlq_event.error_message,
        "raw_bytes": dlq_event.raw_bytes,
    }
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, schema, record)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# KafkaPublisher
# ---------------------------------------------------------------------------

_MAX_RETRIES = 3
_RETRY_DELAY_S = 0.5


class KafkaPublisher:
    """Serialize events to Avro and produce to Kafka with retry logic."""

    def __init__(self, bootstrap_servers: str, schema_registry_url: str) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._schema_registry_url = schema_registry_url
        self._event_schema = _load_schema("event.avsc")
        self._dlq_schema = _load_schema("dlq_event.avsc")
        self._producer = self._build_producer()

    # ------------------------------------------------------------------
    # Producer factory — separated so tests can override it easily
    # ------------------------------------------------------------------

    def _build_producer(self):  # type: ignore[return]
        try:
            from confluent_kafka import Producer  # type: ignore
            return Producer({"bootstrap.servers": self._bootstrap_servers})
        except ImportError:
            _log.warning("confluent_kafka not installed; Kafka delivery disabled")
            return None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def publish(self, topic: str, event: Event) -> None:
        """Serialize *event* to Avro and produce to *topic*; retry up to 3 times."""
        try:
            avro_bytes = _serialize_event(event, self._event_schema)
        except Exception as exc:
            _log.error(
                "Avro serialization failed; dropping event",
                event_id=event.event_id,
                error=str(exc),
            )
            return

        self._produce_with_retry(topic, avro_bytes, event.event_id)

    def publish_dlq(self, topic: str, event: Event, error: Exception) -> None:
        """Wrap *event* in a DLQEvent envelope and produce to *topic*."""
        # Attempt to get raw Avro bytes; fall back to JSON bytes
        try:
            raw = _serialize_event(event, self._event_schema)
        except Exception:
            raw = json.dumps({
                "event_id": event.event_id,
                "source": event.source,
                "timestamp": event.timestamp,
                "payload": event.payload,
            }).encode()

        dlq_event = DLQEvent(
            original_topic=topic,
            original_partition=0,
            original_offset=-1,
            error_type=type(error).__name__,
            error_message=str(error),
            raw_bytes=raw,
        )

        try:
            dlq_bytes = _serialize_dlq_event(dlq_event, self._dlq_schema)
        except Exception as exc:
            _log.error(
                "DLQ envelope serialization failed; dropping event",
                event_id=event.event_id,
                error=str(exc),
            )
            return

        dlq_topic = f"{topic}.dlq"
        self._produce_with_retry(dlq_topic, dlq_bytes, event.event_id)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _produce_with_retry(self, topic: str, payload: bytes, event_id: str) -> None:
        """Produce *payload* to *topic*, retrying up to _MAX_RETRIES times."""
        last_exc: Exception | None = None
        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                self._do_produce(topic, payload)
                _log.info("Event produced", event_id=event_id, topic=topic, attempt=attempt)
                return
            except Exception as exc:
                last_exc = exc
                _log.error(
                    "Kafka produce failed",
                    event_id=event_id,
                    topic=topic,
                    attempt=attempt,
                    error=str(exc),
                )
                if attempt < _MAX_RETRIES:
                    time.sleep(_RETRY_DELAY_S)

        _log.error(
            "All retries exhausted; dropping event",
            event_id=event_id,
            topic=topic,
            error=str(last_exc),
        )

    def _do_produce(self, topic: str, payload: bytes) -> None:
        """Low-level produce call; raises on failure."""
        if self._producer is None:
            raise RuntimeError("No Kafka producer available")
        self._producer.produce(topic, value=payload)
        self._producer.flush()
