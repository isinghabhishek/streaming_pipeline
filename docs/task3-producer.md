# Task 3 — Python Producer: Files Created & Found

## Files Created

### `producer/adapters/`

| File | Description |
|---|---|
| `producer/adapters/__init__.py` | Package init; exports `RawDataPoint`, `SourceAdapter`, `RestApiAdapter`, `IoTSimulatorAdapter`, `WebSocketAdapter` |
| `producer/adapters/base.py` | `RawDataPoint` dataclass (`source`, `value`, `raw`) and `SourceAdapter` Protocol defining `fetch() -> list[RawDataPoint]` |
| `producer/adapters/rest_api.py` | `RestApiAdapter` — polls a REST endpoint via `requests.get`; exponential backoff on `ConnectionError`/`Timeout` (start 1 s, double, cap 60 s); structured JSON error logging |
| `producer/adapters/iot_simulator.py` | `IoTSimulatorAdapter` — generates synthetic temperature / humidity / pressure readings; no external connectivity, no backoff |
| `producer/adapters/websocket.py` | `WebSocketAdapter` — collects messages from a WebSocket URL via `websockets`; same exponential backoff and structured JSON error logging as REST adapter |

### `producer/`

| File | Description |
|---|---|
| `producer/publisher.py` | `Event` dataclass, `DLQEvent` dataclass, and `KafkaPublisher` class with `publish()` and `publish_dlq()` methods; Avro serialization via `fastavro`; up to 3 retries at 500 ms intervals; routes failures to `<topic>.dlq`; structured JSON logging throughout |
| `producer/main.py` | `_require_env()` helper (exits code 1 with structured log on missing var), `build_adapter()` factory, `run_loop()` publish loop, and `main()` entry point; reads `SOURCE_TYPE`, `SOURCE_URL`, `KAFKA_BOOTSTRAP_SERVERS`, `SCHEMA_REGISTRY_URL`, `POLL_INTERVAL_MS` from environment |

### `tests/`

| File | Description |
|---|---|
| `tests/test_adapters.py` | 19 unit tests covering `RawDataPoint` fields, protocol conformance, IoT sensor value ranges, REST backoff reset/cap, WebSocket backoff, and structured JSON error log format |
| `tests/test_publisher.py` | 15 unit tests covering Avro serialization round-trips, retry behaviour (fail-then-succeed, all-retries-fail), DLQ envelope construction, topic routing to `<topic>.dlq`, and structured log format |
| `tests/test_main.py` | 31 unit tests covering missing env var exits (code 1, descriptive log), correct adapter per source type, topic name equals `SOURCE_TYPE`, payload string coercion, UUID `event_id`, ISO-8601 `timestamp`, per-event structured logging |
| `tests/test_property_2_events_per_data_point.py` | **Property 2** — Hypothesis property test: generates 1–100 data points, asserts `publish` is called exactly N times per iteration. Validates Requirement 1.3 |
| `tests/test_property_3_source_topic_routing.py` | **Property 3** — Hypothesis property test: generates random source types, asserts every `publish` call uses that source type as the topic name. Validates Requirement 1.6 |
| `tests/test_property_4_dlq_metadata.py` | **Property 4** — Hypothesis property test: generates random `Event` + `Exception` pairs, calls `publish_dlq`, deserializes the captured bytes, and asserts all 6 required DLQ fields are present and non-empty. Validates Requirements 2.3, 8.2 |
| `tests/test_producer_unit.py` | 23 unit tests covering: backoff doubling/reset/cap (3 adapters), DLQ envelope field values (`original_topic`, `error_type`, `error_message`, `raw_bytes`), structured JSON log format (all 4 required fields), and missing env var exit(1) for all 4 required variables |

---

## Files Found (Pre-existing, Referenced by Task 3)

| File | Used By |
|---|---|
| `schemas/event.avsc` | `producer/publisher.py` — loads Event Avro schema for serialization |
| `schemas/dlq_event.avsc` | `producer/publisher.py` — loads DLQEvent Avro schema for DLQ envelope serialization |
| `producer/__init__.py` | Pre-existing package marker; not modified |
| `tests/__init__.py` | Pre-existing package marker; not modified |
| `tests/conftest.py` | Pre-existing pytest fixtures; used by all test files |

---

## Directory Structure After Task 3

```
producer/
├── __init__.py              (pre-existing)
├── main.py                  ← created (3.3)
├── publisher.py             ← created (3.2)
└── adapters/
    ├── __init__.py          ← created (3.1)
    ├── base.py              ← created (3.1)
    ├── iot_simulator.py     ← created (3.1)
    ├── rest_api.py          ← created (3.1)
    └── websocket.py         ← created (3.1)

schemas/
├── event.avsc               (pre-existing, referenced)
└── dlq_event.avsc           (pre-existing, referenced)

tests/
├── __init__.py              (pre-existing)
├── conftest.py              (pre-existing)
├── test_adapters.py         ← created (3.1)
├── test_publisher.py        ← created (3.2)
├── test_main.py             ← created (3.3)
├── test_property_2_events_per_data_point.py  ← created (3.4)
├── test_property_3_source_topic_routing.py   ← created (3.5)
├── test_property_4_dlq_metadata.py           ← created (3.6)
└── test_producer_unit.py                     ← created (3.7)
```

---

## Key Design Decisions

- **Avro serialization** uses `fastavro` (schemaless writer/reader) rather than a live Schema Registry client, keeping tests runnable without a running registry service.
- **Exponential backoff** is implemented directly in each adapter (`_backoff` instance variable); resets to 1 s on success, doubles on each failure, caps at 60 s.
- **DLQ topic naming** follows the convention `<original-topic>.dlq` (e.g., `rest_api.dlq`).
- **Kafka topic name** is derived directly from `SOURCE_TYPE` (e.g., `SOURCE_TYPE=iot_simulator` → topic `iot_simulator`).
- **Structured logging** emits JSON to stdout with fields: `timestamp`, `component`, `level`, `message`, and optionally `event_id`.
