# Feature: real-time-streaming-pipeline, Property 4: DLQ metadata completeness
"""
Property 4: Invalid events are routed to the DLQ with complete metadata

For any Event and any Exception, publish_dlq must produce a DLQ message that
contains all six required metadata fields: original_topic, original_partition,
original_offset, error_type, error_message, raw_bytes.

Validates: Requirements 2.3, 8.2
"""
from __future__ import annotations

import io
import json
import os
from unittest.mock import patch

import fastavro
from hypothesis import given, settings
from hypothesis import strategies as st

from producer.publisher import Event, KafkaPublisher, _load_schema

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_DLQ_SCHEMA = _load_schema("dlq_event.avsc")

# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

_text_st = st.text(min_size=1, max_size=50)
_payload_st = st.dictionaries(_text_st, _text_st, max_size=5)

_event_st = st.builds(
    Event,
    event_id=st.uuids().map(str),
    source=st.sampled_from(["rest_api", "iot_simulator", "websocket"]),
    timestamp=st.datetimes().map(lambda d: d.isoformat() + "Z"),
    payload=_payload_st,
)

_exception_st = st.one_of(
    st.builds(ValueError, _text_st),
    st.builds(RuntimeError, _text_st),
    st.builds(KeyError, _text_st),
    st.builds(TypeError, _text_st),
)

_topic_st = st.sampled_from(["rest_api", "iot_simulator", "websocket"])

# ---------------------------------------------------------------------------
# Helper: build a KafkaPublisher without a live Kafka broker
# ---------------------------------------------------------------------------

def _make_publisher():
    pub = KafkaPublisher.__new__(KafkaPublisher)
    pub._bootstrap_servers = "localhost:9092"
    pub._schema_registry_url = "http://localhost:8081"
    pub._event_schema = _load_schema("event.avsc")
    pub._dlq_schema = _DLQ_SCHEMA
    pub._producer = None  # will be patched
    return pub


# ---------------------------------------------------------------------------
# Property test
# ---------------------------------------------------------------------------

@given(topic=_topic_st, event=_event_st, error=_exception_st)
@settings(max_examples=100)
def test_dlq_metadata_completeness(topic, event, error):
    """Validates: Requirements 2.3, 8.2"""
    captured: list[bytes] = []

    def fake_do_produce(t: str, payload: bytes) -> None:
        captured.append(payload)

    pub = _make_publisher()

    with patch.object(pub, "_do_produce", side_effect=fake_do_produce):
        pub.publish_dlq(topic, event, error)

    assert len(captured) == 1, "Expected exactly one DLQ message to be produced"

    dlq_bytes = captured[0]
    record = fastavro.schemaless_reader(io.BytesIO(dlq_bytes), _DLQ_SCHEMA)

    # All six required fields must be present and non-empty / non-None
    assert record.get("original_topic"), "original_topic must be non-empty"
    assert record.get("error_type"), "error_type must be non-empty"
    assert record.get("error_message") is not None, "error_message must be present"
    assert isinstance(record.get("raw_bytes"), (bytes, bytearray)), "raw_bytes must be bytes"
    assert len(record["raw_bytes"]) > 0, "raw_bytes must be non-empty"
    assert "original_partition" in record, "original_partition must be present"
    assert "original_offset" in record, "original_offset must be present"

    # Verify field values match the inputs
    assert record["original_topic"] == topic
    assert record["error_type"] == type(error).__name__
    assert record["error_message"] == str(error)
