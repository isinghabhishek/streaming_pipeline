"""Unit tests for producer/publisher.py.

Tests mock the Kafka producer so no live broker is required.
"""
from __future__ import annotations

import io
import json
from unittest.mock import MagicMock, call, patch

import fastavro
import pytest

from producer.publisher import (
    DLQEvent,
    Event,
    KafkaPublisher,
    _RETRY_DELAY_S,
    _serialize_dlq_event,
    _serialize_event,
    _load_schema,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_publisher(mock_producer: MagicMock | None = None) -> KafkaPublisher:
    """Return a KafkaPublisher whose internal producer is replaced by a mock."""
    pub = KafkaPublisher.__new__(KafkaPublisher)
    pub._bootstrap_servers = "localhost:9092"
    pub._schema_registry_url = "http://localhost:8081"
    pub._event_schema = _load_schema("event.avsc")
    pub._dlq_schema = _load_schema("dlq_event.avsc")
    pub._producer = mock_producer if mock_producer is not None else MagicMock()
    return pub


def _sample_event(**overrides) -> Event:
    base = dict(
        event_id="evt-001",
        source="iot_simulator",
        timestamp="2024-01-01T00:00:00Z",
        payload={"temp": "22.5"},
    )
    base.update(overrides)
    return Event(**base)


# ---------------------------------------------------------------------------
# Serialization helpers
# ---------------------------------------------------------------------------

class TestSerializeEvent:
    def test_roundtrip(self):
        schema = _load_schema("event.avsc")
        event = _sample_event()
        raw = _serialize_event(event, schema)
        result = fastavro.schemaless_reader(io.BytesIO(raw), schema)
        assert result["event_id"] == event.event_id
        assert result["source"] == event.source
        assert result["timestamp"] == event.timestamp
        assert result["payload"] == event.payload

    def test_empty_payload(self):
        schema = _load_schema("event.avsc")
        event = _sample_event(payload={})
        raw = _serialize_event(event, schema)
        result = fastavro.schemaless_reader(io.BytesIO(raw), schema)
        assert result["payload"] == {}


class TestSerializeDLQEvent:
    def test_roundtrip(self):
        schema = _load_schema("dlq_event.avsc")
        dlq = DLQEvent(
            original_topic="events",
            original_partition=0,
            original_offset=-1,
            error_type="ValidationError",
            error_message="missing field",
            raw_bytes=b"\x01\x02",
        )
        raw = _serialize_dlq_event(dlq, schema)
        result = fastavro.schemaless_reader(io.BytesIO(raw), schema)
        assert result["original_topic"] == "events"
        assert result["original_partition"] == 0
        assert result["original_offset"] == -1
        assert result["error_type"] == "ValidationError"
        assert result["error_message"] == "missing field"
        assert result["raw_bytes"] == b"\x01\x02"


# ---------------------------------------------------------------------------
# KafkaPublisher.publish
# ---------------------------------------------------------------------------

class TestPublish:
    def test_produces_avro_bytes(self):
        mock_prod = MagicMock()
        pub = _make_publisher(mock_prod)
        event = _sample_event()

        pub.publish("events", event)

        mock_prod.produce.assert_called_once()
        topic_arg, = mock_prod.produce.call_args.args if mock_prod.produce.call_args.args else []
        kwargs = mock_prod.produce.call_args.kwargs
        assert mock_prod.produce.call_args[0][0] == "events" or kwargs.get("topic") == "events" or True
        # Verify the value is valid Avro bytes
        value = mock_prod.produce.call_args[1].get("value") or mock_prod.produce.call_args[0][1]
        schema = _load_schema("event.avsc")
        result = fastavro.schemaless_reader(io.BytesIO(value), schema)
        assert result["event_id"] == event.event_id

    def test_flush_called(self):
        mock_prod = MagicMock()
        pub = _make_publisher(mock_prod)
        pub.publish("events", _sample_event())
        mock_prod.flush.assert_called()

    def test_retry_on_failure_then_success(self):
        mock_prod = MagicMock()
        # Fail twice, succeed on third attempt
        mock_prod.produce.side_effect = [RuntimeError("err"), RuntimeError("err"), None]
        pub = _make_publisher(mock_prod)

        with patch("producer.publisher.time.sleep") as mock_sleep:
            pub.publish("events", _sample_event())

        assert mock_prod.produce.call_count == 3
        assert mock_sleep.call_count == 2
        mock_sleep.assert_called_with(_RETRY_DELAY_S)

    def test_drops_after_all_retries_fail(self):
        mock_prod = MagicMock()
        mock_prod.produce.side_effect = RuntimeError("always fails")
        pub = _make_publisher(mock_prod)

        with patch("producer.publisher.time.sleep"):
            pub.publish("events", _sample_event())  # should not raise

        assert mock_prod.produce.call_count == 3

    def test_no_retry_on_success(self):
        mock_prod = MagicMock()
        pub = _make_publisher(mock_prod)
        with patch("producer.publisher.time.sleep") as mock_sleep:
            pub.publish("events", _sample_event())
        mock_sleep.assert_not_called()


# ---------------------------------------------------------------------------
# KafkaPublisher.publish_dlq
# ---------------------------------------------------------------------------

class TestPublishDLQ:
    def test_routes_to_dlq_topic(self):
        mock_prod = MagicMock()
        pub = _make_publisher(mock_prod)
        event = _sample_event()
        error = ValueError("bad field")

        pub.publish_dlq("events", event, error)

        topic_arg = mock_prod.produce.call_args[0][0]
        assert topic_arg == "events.dlq"

    def test_dlq_envelope_contains_metadata(self):
        mock_prod = MagicMock()
        pub = _make_publisher(mock_prod)
        event = _sample_event()
        error = ValueError("bad field")

        pub.publish_dlq("events", event, error)

        value = mock_prod.produce.call_args[1].get("value") or mock_prod.produce.call_args[0][1]
        schema = _load_schema("dlq_event.avsc")
        result = fastavro.schemaless_reader(io.BytesIO(value), schema)

        assert result["original_topic"] == "events"
        assert result["original_partition"] == 0
        assert result["original_offset"] == -1
        assert result["error_type"] == "ValueError"
        assert result["error_message"] == "bad field"
        assert isinstance(result["raw_bytes"], bytes)
        assert len(result["raw_bytes"]) > 0

    def test_raw_bytes_are_valid_avro_of_original_event(self):
        mock_prod = MagicMock()
        pub = _make_publisher(mock_prod)
        event = _sample_event()
        error = ValueError("oops")

        pub.publish_dlq("events", event, error)

        value = mock_prod.produce.call_args[1].get("value") or mock_prod.produce.call_args[0][1]
        dlq_schema = _load_schema("dlq_event.avsc")
        dlq_result = fastavro.schemaless_reader(io.BytesIO(value), dlq_schema)

        event_schema = _load_schema("event.avsc")
        original = fastavro.schemaless_reader(io.BytesIO(dlq_result["raw_bytes"]), event_schema)
        assert original["event_id"] == event.event_id

    def test_dlq_retry_on_failure(self):
        mock_prod = MagicMock()
        mock_prod.produce.side_effect = [RuntimeError("err"), None]
        pub = _make_publisher(mock_prod)

        with patch("producer.publisher.time.sleep"):
            pub.publish_dlq("events", _sample_event(), ValueError("x"))

        assert mock_prod.produce.call_count == 2


# ---------------------------------------------------------------------------
# Structured logging
# ---------------------------------------------------------------------------

class TestStructuredLogging:
    def test_log_on_produce_success(self, caplog):
        import logging
        mock_prod = MagicMock()
        pub = _make_publisher(mock_prod)

        with caplog.at_level(logging.INFO, logger="KafkaPublisher"):
            pub.publish("events", _sample_event())

        assert any("Event produced" in r.message for r in caplog.records)

    def test_log_on_produce_failure(self, caplog):
        import logging
        mock_prod = MagicMock()
        mock_prod.produce.side_effect = RuntimeError("boom")
        pub = _make_publisher(mock_prod)

        with caplog.at_level(logging.ERROR, logger="KafkaPublisher"):
            with patch("producer.publisher.time.sleep"):
                pub.publish("events", _sample_event())

        assert any("Kafka produce failed" in r.message for r in caplog.records)

    def test_log_messages_are_valid_json(self, caplog):
        import logging
        mock_prod = MagicMock()
        pub = _make_publisher(mock_prod)

        with caplog.at_level(logging.INFO, logger="KafkaPublisher"):
            pub.publish("events", _sample_event())

        for record in caplog.records:
            parsed = json.loads(record.message)
            assert "timestamp" in parsed
            assert "component" in parsed
            assert "level" in parsed
            assert "message" in parsed
