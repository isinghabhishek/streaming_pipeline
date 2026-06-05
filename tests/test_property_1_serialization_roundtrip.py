# Feature: real-time-streaming-pipeline, Property 1: Event serialization round-trip
"""
Property 1: Event serialization round-trip

For any valid Event object, serializing it to Avro using fastavro and then
deserializing it should produce an object equal to the original.

Validates: Requirements 1.5, 2.2
"""
import io
import json
import os

import fastavro
from hypothesis import given, settings
from hypothesis import strategies as st

# Load the schema from schemas/event.avsc relative to project root
_SCHEMA_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "schemas",
    "event.avsc",
)

with open(_SCHEMA_PATH) as _f:
    _PARSED_SCHEMA = fastavro.parse_schema(json.load(_f))

# Generators
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
def test_event_serialization_roundtrip(event_id, source, timestamp, payload):
    """Validates: Requirements 1.5, 2.2"""
    event = {
        "event_id": event_id,
        "source": source,
        "timestamp": timestamp,
        "payload": payload,
    }

    # Serialize to Avro bytes
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, _PARSED_SCHEMA, event)
    avro_bytes = buf.getvalue()

    # Deserialize back
    buf.seek(0)
    result = fastavro.schemaless_reader(buf, _PARSED_SCHEMA)

    assert result == event, (
        f"Round-trip failed.\nOriginal: {event}\nDeserialized: {result}"
    )
