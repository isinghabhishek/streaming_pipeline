# Implementation Plan: Real-Time Streaming Pipeline

## Overview

Implement the pipeline incrementally: project scaffolding and Docker Compose first, then the Python Producer, Flink stream processor, Delta Lake storage layer, dbt transformations, Grafana dashboard, and finally observability and utility scripts. Each phase wires into the previous one so the system is runnable end-to-end after each major step.

## Tasks

- [x] 1. Scaffold project structure and Docker Compose environment
  - Create top-level directory layout: `producer/`, `flink-job/`, `storage/`, `dbt/`, `grafana/`, `scripts/`
  - Write `docker-compose.yml` with service definitions for Kafka (KRaft mode), Confluent Schema Registry, Flink JobManager + TaskManager, Spark Thrift Server (Delta Lake), dbt runner (cron), and Grafana
  - Add health-check definitions for each service so `docker compose up --wait` can confirm readiness
  - Write a startup init container / script that registers Avro schemas in the Schema Registry via its REST API
  - Add `.env.example` listing all required environment variables with descriptions
  - _Requirements: 9.4, 10.1, 10.2_

- [x] 2. Define Avro schemas and Schema Registry integration
  - [x] 2.1 Write the `Event` Avro schema JSON (`schemas/event.avsc`) matching the data model
    - Fields: `event_id` (string/UUID), `source` (string), `timestamp` (string/ISO-8601), `payload` (map<string,string>)
    - _Requirements: 2.1, 2.4_
  - [x] 2.2 Write the `DLQEvent` Avro schema JSON (`schemas/dlq_event.avsc`)
    - Fields: `original_topic`, `original_partition`, `original_offset`, `error_type`, `error_message`, `raw_bytes`
    - _Requirements: 8.1, 8.2_
  - [x] 2.3 Write the schema registration init script (`scripts/register_schemas.py`) that POSTs both schemas to the Schema Registry and verifies backward compatibility on re-registration
    - _Requirements: 2.1, 2.5_
  - [x]* 2.4 Write property test for Event serialization round-trip (Property 1)
    - **Property 1: Event serialization round-trip**
    - Use Hypothesis to generate random valid `Event` objects, serialize to Avro via Schema Registry client, deserialize, and assert equality
    - Tag: `# Feature: real-time-streaming-pipeline, Property 1: Event serialization round-trip`
    - **Validates: Requirements 1.5, 2.2**
  - [x]* 2.5 Write property test for schema backward compatibility (Property 5)
    - **Property 5: Schema backward compatibility round-trip**
    - Use Hypothesis to generate random v1 events, deserialize with v2 schema, assert all v1 fields preserved
    - Tag: `# Feature: real-time-streaming-pipeline, Property 5: Schema backward compatibility round-trip`
    - **Validates: Requirements 2.5**

- [x] 3. Implement the Python Producer
  - [x] 3.1 Define the `SourceAdapter` protocol and implement three adapters: `RestApiAdapter`, `IoTSimulatorAdapter`, `WebSocketAdapter` in `producer/adapters/`
    - Each `fetch()` returns a `list[RawDataPoint]`
    - Implement exponential backoff (start 1 s, double, cap 60 s) for connectivity failures; log structured errors
    - _Requirements: 1.1, 1.4_
  - [x] 3.2 Implement `KafkaPublisher` in `producer/publisher.py`
    - `publish(topic, event)` — serialize event to Avro via Schema Registry client and produce to Kafka
    - `publish_dlq(topic, event, error)` — wrap in `DLQEvent` envelope and produce to DLQ topic
    - Retry Kafka publish up to 3 times with 500 ms delay; log at ERROR and drop after all retries fail
    - _Requirements: 1.3, 1.5, 2.3, 8.1, 8.2_
  - [x] 3.3 Implement the main Producer loop in `producer/main.py`
    - Read `SOURCE_TYPE`, `SOURCE_URL`, `KAFKA_BOOTSTRAP_SERVERS`, `SCHEMA_REGISTRY_URL`, `POLL_INTERVAL_MS` from environment; exit with code 1 and descriptive log if any required variable is missing
    - Route each source type to its dedicated Kafka topic named after the source type
    - Begin publishing within 10 s of startup
    - Emit structured JSON logs (`timestamp`, `component`, `level`, `message`, `event_id`)
    - _Requirements: 1.2, 1.6, 9.1, 10.5_
  - [x]* 3.4 Write property test for one event published per data point (Property 2)
    - **Property 2: One event published per data point**
    - Use Hypothesis to generate source responses with 1–100 data points; assert exactly N events published
    - Tag: `# Feature: real-time-streaming-pipeline, Property 2: One event published per data point`
    - **Validates: Requirements 1.3**
  - [x]* 3.5 Write property test for source-to-topic routing (Property 3)
    - **Property 3: Source-to-topic routing**
    - Use Hypothesis to generate random sets of source types; assert each event lands on the matching topic
    - Tag: `# Feature: real-time-streaming-pipeline, Property 3: Source-to-topic routing`
    - **Validates: Requirements 1.6**
  - [x]* 3.6 Write property test for DLQ metadata completeness (Property 4)
    - **Property 4: Invalid events are routed to the DLQ with complete metadata**
    - Use Hypothesis to generate invalid events (missing/wrong-type fields); assert DLQ message contains all required metadata fields
    - Tag: `# Feature: real-time-streaming-pipeline, Property 4: DLQ metadata completeness`
    - **Validates: Requirements 2.3, 8.2**
  - [x]* 3.7 Write unit tests for Producer
    - Test exponential backoff interval calculation
    - Test DLQ envelope construction
    - Test structured log output format
    - Test missing env var exit behavior
    - _Requirements: 1.4, 8.2, 9.1, 10.5_

- [ ] 4. Checkpoint — Ensure all Producer tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 5. Implement the Flink Stream Processor
  - [ ] 5.1 Set up the Flink Maven/Gradle project in `flink-job/` with dependencies: Flink Kafka connector, Flink Delta connector, jqwik
    - _Requirements: 3.1_
  - [ ] 5.2 Implement `KafkaSource` configuration with consumer group, committed offsets per partition, and event-time watermark strategy based on the `timestamp` field
    - _Requirements: 3.1, 3.2_
  - [ ] 5.3 Implement `ParseMap` operator that deserializes Avro events; on parse failure, routes to DLQ and increments `parse_errors` counter
    - _Requirements: 3.2_
  - [ ] 5.4 Implement `AggregateFunction` for tumbling-window aggregations (count, sum, avg) with configurable window size (10–300 s); emit result when watermark advances past window boundary
    - _Requirements: 3.3, 3.4_
  - [ ]* 5.5 Write property test for windowed aggregation correctness (Property 6)
    - **Property 6: Windowed aggregation correctness and emission**
    - Use jqwik to generate random event sets and window sizes 10–300 s; assert emitted aggregation equals mathematically correct value
    - Tag: `# Feature: real-time-streaming-pipeline, Property 6: Windowed aggregation correctness and emission`
    - **Validates: Requirements 3.3, 3.4**
  - [ ] 5.6 Implement `DeltaSink` for `raw_events` and `aggregations` tables using the Flink Delta connector with two-phase commit (exactly-once)
    - Deduplicate events by `event_id` within the processing window using Flink keyed state
    - _Requirements: 3.5, 4.1, 4.3_
  - [ ]* 5.7 Write property test for exactly-once write count (Property 8)
    - **Property 8: Exactly-once write count**
    - Use jqwik to generate random streams with 0–50% duplicate `event_id`s; assert records written equals unique `event_id` count
    - Tag: `# Feature: real-time-streaming-pipeline, Property 8: Exactly-once write count`
    - **Validates: Requirements 4.3, 4.4, 4.5**
  - [ ] 5.8 Configure Flink checkpointing: interval from `CHECKPOINT_INTERVAL_MS`, save to durable volume, complete within 30 s; transition job to FAILED after 3 consecutive checkpoint failures
    - Commit Kafka consumer offsets atomically with the Delta write transaction on checkpoint completion
    - _Requirements: 3.6, 3.7, 4.2_
  - [ ]* 5.9 Write property test for checkpoint recovery (Property 7)
    - **Property 7: Checkpoint recovery preserves exactly-once semantics**
    - Use jqwik to generate random event sequences with injected mid-stream failures; assert final Table Store state equals uninterrupted processing
    - Tag: `# Feature: real-time-streaming-pipeline, Property 7: Checkpoint recovery preserves exactly-once semantics`
    - **Validates: Requirements 3.7, 4.1, 4.2**
  - [ ] 5.10 Implement the health HTTP endpoint on port 8081 returning 200 when running, 503 when stopped or in error state
    - Expose Prometheus-compatible metrics: records consumed/s, records written/s, checkpoint duration, processing lag per partition
    - _Requirements: 3.8, 9.2_

- [ ] 6. Checkpoint — Ensure all Flink job tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 7. Implement Delta Lake storage layer
  - [ ] 7.1 Write PySpark initialization script (`storage/init_tables.py`) that creates `raw_events` and `aggregations` Delta tables with the defined schemas and partition columns
    - `raw_events`: partitioned by `source` and `date`; `aggregations`: partitioned by `source` and `window_start`
    - _Requirements: 5.1, 5.5_
  - [ ] 7.2 Verify ACID transaction support and configure table properties: `delta.logRetentionDuration = interval 7 days`, optimistic concurrency control
    - _Requirements: 5.2, 5.3, 5.6_
  - [ ]* 7.3 Write property test for time-travel correctness (Property 9)
    - **Property 9: Table Store time-travel correctness**
    - Use Hypothesis to generate random write sequences (1–20 batches); assert querying at version V returns data present after the V-th write
    - Tag: `# Feature: real-time-streaming-pipeline, Property 9: Table Store time-travel correctness`
    - **Validates: Requirements 5.3, 5.4**
  - [ ]* 7.4 Write property test for partition correctness (Property 10)
    - **Property 10: Partition correctness**
    - Use Hypothesis to generate random events with varied sources and timestamps; assert each event appears in the correct partition
    - Tag: `# Feature: real-time-streaming-pipeline, Property 10: Partition correctness`
    - **Validates: Requirements 5.5**

- [ ] 8. Implement dbt transformations
  - [ ] 8.1 Write `dbt/models/staging/stg_raw_events.sql` — light cleaning of `raw_events` (cast types, filter nulls)
    - _Requirements: 6.1_
  - [ ] 8.2 Write `dbt/models/marts/events_per_minute.sql` — per-source event counts per minute, writing to a dedicated schema without touching `raw_events`
    - _Requirements: 6.1, 6.2_
  - [ ] 8.3 Write dbt data tests in `dbt/tests/`: not-null `event_id`, unique `event_id` within partition, `timestamp` within valid range; configure dbt to exit non-zero on test failure and log test name + row count
    - _Requirements: 6.3, 6.4_
  - [ ] 8.4 Write the cron scheduler entry in `docker-compose.yml` that runs `dbt run && dbt test` every 5 minutes
    - _Requirements: 6.5_
  - [ ]* 8.5 Write property test for dbt immutability (Property 11)
    - **Property 11: dbt run does not modify raw_events**
    - Use Hypothesis to generate random `raw_events` datasets; assert row count and content hash of `raw_events` are unchanged after dbt run
    - Tag: `# Feature: real-time-streaming-pipeline, Property 11: dbt run does not modify raw_events`
    - **Validates: Requirements 6.2**
  - [ ]* 8.6 Write property test for dbt test violation detection (Property 12)
    - **Property 12: dbt data tests detect violations**
    - Use Hypothesis to generate datasets with injected null `event_id`, duplicate `event_id`, or out-of-range `timestamp`; assert dbt tests fail and report violating test name and row count
    - Tag: `# Feature: real-time-streaming-pipeline, Property 12: dbt data tests detect violations`
    - **Validates: Requirements 6.3, 6.4**

- [ ] 9. Implement Grafana dashboard and alerting
  - [ ] 9.1 Write Grafana datasource provisioning file `grafana/provisioning/datasources/delta.yaml` pointing to the Spark Thrift Server
    - _Requirements: 7.3_
  - [ ] 9.2 Write `grafana/provisioning/dashboards/pipeline.json` with panels: events ingested per minute, processing latency (p50/p95), aggregated metric values per source, DLQ event count
    - Configure auto-refresh ≤ 60 s; wire time range selector to all panels
    - _Requirements: 7.1, 7.2, 7.4_
  - [ ] 9.3 Add alert rule to the dashboard JSON: trigger when DLQ event count > 10 within a 5-minute window
    - _Requirements: 7.5_

- [ ] 10. Implement utility scripts and observability
  - [ ] 10.1 Write `scripts/seed.py` — generates synthetic Avro-conformant events and publishes them directly to Kafka; accepts a configurable event count argument
    - _Requirements: 10.3_
  - [ ] 10.2 Write `scripts/reprocess_dlq.py` — reads from the DLQ topic, attempts schema correction, republishes corrected events to the original topic, logs each reprocessed event
    - _Requirements: 8.5_
  - [ ]* 10.3 Write property test for DLQ reprocessing round-trip (Property 13)
    - **Property 13: DLQ reprocessing round-trip**
    - Use Hypothesis to generate random correctable and uncorrectable DLQ events; assert correctable events appear on the original topic with valid schema
    - Tag: `# Feature: real-time-streaming-pipeline, Property 13: DLQ reprocessing round-trip`
    - **Validates: Requirements 8.5**
  - [ ]* 10.4 Write property test for structured log format (Property 14)
    - **Property 14: Structured log format**
    - Use Hypothesis to generate random log-triggering operations across Producer, Stream Processor, and dbt runner; assert each emitted log line is valid JSON with required fields
    - Tag: `# Feature: real-time-streaming-pipeline, Property 14: Structured log format`
    - **Validates: Requirements 9.1**
  - [ ]* 10.5 Write property test for missing env var failure (Property 15)
    - **Property 15: Missing environment variable causes descriptive failure**
    - Use Hypothesis to generate random subsets of required env vars; assert component exits non-zero and log contains the missing variable name
    - Tag: `# Feature: real-time-streaming-pipeline, Property 15: Missing environment variable causes descriptive failure`
    - **Validates: Requirements 10.5**

- [ ] 11. Write README and smoke tests
  - [ ] 11.1 Write `README.md` with: step-by-step setup instructions, architecture diagram description, and explanation of the exactly-once semantics implementation
    - _Requirements: 10.4_
  - [ ] 11.2 Write smoke tests (`tests/smoke/`) that assert: Schema Registry has registered schemas for each topic, Event Avro schema contains required fields, DLQ topic exists with retention ≥ 72 h, Grafana provisioning files exist with required panel titles and alert rule, `docker-compose.yml` contains all required service definitions
    - _Requirements: 2.1, 7.3, 8.1, 8.3, 10.1_

- [ ] 12. Final checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for a faster MVP
- Each task references specific requirements for traceability
- Property tests use Hypothesis (Python) and jqwik (Java/Flink); each runs a minimum of 100 iterations
- Checkpoints ensure incremental validation after each major component
- The system is designed to run entirely via `docker compose up` — no cloud infrastructure required
