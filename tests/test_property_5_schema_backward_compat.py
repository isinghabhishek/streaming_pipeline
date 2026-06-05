# Feature: real-time-streaming-pipeline, Property 5: Schema backward compatibility round-trip
"""
Property 5: Schema backward compatibility round-trip

For any event serialized with schema version v1, deserializing it using a v2
schema (backward-compatible update — adds an optional field with a default)
should succeed and preserve all v1 field values.

Validates: Requirements 2.5
"""
import io

import fastavro
from hypothesis import given, settings
from hypothesis import strategies as st

# v1 schema — same as schemas/event.avsc
_V1_SCHEMA_DICT = {
    "type": "record",
    "name": "Event",
    "namespace": "com.pipeline",
    "fields": [
        {"name": "event_id", "type": "string"},
        {"name": "source", "type": "string"},
        {"name": "timestamp", "type": "string"},
        {"name": "payload", "type": {"type": "map", "values": "string"}},
    ],
}

# v2 schema — backward-compatible addition of optional "version" field
_V2_SCHEMA_DICT = {
    "type": "record",
    "name": "Event",
    "namespace": "com.pipeline",
    "fields": [
        {"name": "event_id", "type": "string"},
        {"name": "source", "type": "string"},
        {"name": "timestamp", "type": "string"},
        {"name": "payload", "type": {"type": "map", "values": "string"}},
        {"name": "version", "type": ["null", "string"], "default": None},
    ],
}

_V1_PARSED = fastavro.parse_schema(_V1_SCHEMA_DICT)
_V2_PARSED = fastavro.parse_schema(_V2_SCHEMA_DICT)

# Generators (same as Property 1)
_event_id_st = st.uuids().map(str)
_source_st = st.text(
    min_size=1,
    max_size=50,
    alphabet=st.characters(
        whitelist_categories=("Lu", "Ll", "Nd"),
        whitelist_characters="_-",
    ),
)
_timestamp_st = st.datetimes().map(lambda d: d.isoformat() + "Z")
_payload_st = st.dictionaries(
    st.text(min_size=1, max_size=20),
    st.text(max_size=100),
    max_size=10,
)


@given(
    event_id=_event_id_st,
    source=_source_st,
    timestamp=_timestamp_st,
    payload=_payload_st,
)
@settings(max_examples=100)
def test_schema_backward_compat_roundtrip(event_id, source, timestamp, payload):
    """Validates: Requirements 2.5"""
    v1_event = {
        "event_id": event_id,
        "source": source,
        "timestamp": timestamp,
        "payload": payload,
    }

    # Serialize with v1 schema
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, _V1_PARSED, v1_event)
    buf.seek(0)

    # Deserialize with v2 schema (backward-compatible read)
    # schemaless_reader(fo, writer_schema, reader_schema)
    result = fastavro.schemaless_reader(buf, _V1_PARSED, _V2_PARSED)

    # All v1 fields must be preserved
    assert result["event_id"] == v1_event["event_id"]
    assert result["source"] == v1_event["source"]
    assert result["timestamp"] == v1_event["timestamp"]
    assert result["payload"] == v1_event["payload"]

    # New optional field defaults to None
    assert result["version"] is None, (
        f"Expected 'version' to be None, got {result['version']!r}"
    )
