# Milestone Log — Real-Time Streaming Pipeline

This document records completed milestones, what was implemented, and any test outputs or results. Updated after each significant task or checkpoint.

---

## Task 2 — Avro Schemas & Schema Registry Integration

**Completed:** 2026-04-22
**Status:** ✅ Done

### What was implemented

#### `schemas/event.avsc`
Avro schema for the main pipeline event. Namespace `com.pipeline`, record name `Event`.

| Field | Avro Type | Notes |
|---|---|---|
| `event_id` | `string` | UUID identifier |
| `source` | `string` | Source system name |
| `timestamp` | `string` | ISO-8601 UTC string |
| `payload` | `map<string,string>` | Arbitrary key-value data |

#### `schemas/dlq_event.avsc`
Avro schema for Dead Letter Queue envelope. Namespace `com.pipeline`, record name `DLQEvent`.

| Field | Avro Type | Notes |
|---|---|---|
| `original_topic` | `string` | Kafka topic the event came from |
| `original_partition` | `int` | Kafka partition number |
| `original_offset` | `long` | Kafka offset of the original message |
| `error_type` | `string` | Classification of the error |
| `error_message` | `string` | Human-readable error detail |
| `raw_bytes` | `bytes` | Original raw message bytes |

#### `scripts/register_schemas.py`
Init script that registers both schemas in Confluent Schema Registry at startup.

Key behaviours:
- Reads `SCHEMA_REGISTRY_URL` (default `http://localhost:8081`), `EVENTS_TOPIC` (default `events`), `DLQ_TOPIC` (default `events.dlq`) from environment
- Polls Schema Registry until ready (up to 10 retries, 3 s apart)
- Checks backward compatibility via `POST /compatibility/subjects/{subject}/versions/latest` before re-registering
- Skips registration and marks failure if schema is not backward compatible
- Registers each schema via `POST /subjects/{subject}/versions`
- Emits structured JSON logs (`timestamp`, `component`, `level`, `message`) to stdout
- Exits with code `1` if any registration fails, `0` on full success

### Test outputs / verification

#### Property tests (Tasks 2.4 & 2.5) — ✅ 2 passed

```
pytest tests/test_property_1_serialization_roundtrip.py tests/test_property_5_schema_backward_compat.py -v
```

| Test | Property | Examples | Result |
|---|---|---|---|
| `test_event_serialization_roundtrip` | Property 1: Event serialization round-trip | 100 | ✅ PASSED |
| `test_schema_backward_compat_roundtrip` | Property 5: Schema backward compatibility round-trip | 100 | ✅ PASSED |

**Duration:** ~3 s  
**Dependencies:** `fastavro>=1.9.0`, `hypothesis>=6.100.0`, `pytest>=8.0.0` (see `requirements-test.txt`)

**Files created:**
- `tests/test_property_1_serialization_roundtrip.py` — generates random Events, serializes/deserializes via fastavro, asserts equality
- `tests/test_property_5_schema_backward_compat.py` — serializes v1 events, reads with v2 schema (adds optional `version: null` field), asserts all v1 fields preserved
- `tests/__init__.py`
- `tests/conftest.py` — adds project root to `sys.path`
- `requirements-test.txt`

**Notable fix:** `fastavro.schemaless_reader` signature is `(fo, writer_schema, reader_schema)` — writer schema is positional arg 1, not a keyword.

### Requirements covered

| Requirement | Description |
|---|---|
| 2.1 | Schema Registry stores a versioned schema for each Kafka topic |
| 2.4 | Event schema includes `event_id`, `source`, `timestamp`, `payload` |
| 2.5 | Backward compatibility verified on re-registration |
| 8.1 | DLQ is a dedicated topic; DLQEvent schema defined |
| 8.2 | DLQ messages include original topic, partition, offset, error type, error message |

---

<!-- Add new milestone entries above this line -->
