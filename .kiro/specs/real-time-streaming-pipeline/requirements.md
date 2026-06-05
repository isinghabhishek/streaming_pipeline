# Requirements Document

## Introduction

A Real-Time Streaming Pipeline is a portfolio and learning project demonstrating modern data engineering patterns. The system ingests live event data from external sources (stock prices, IoT sensors, or public APIs), routes it through Apache Kafka, processes it with Apache Flink or Spark Streaming, persists results to a Delta Lake or Apache Iceberg table storage layer, transforms data with dbt, and visualizes metrics in a Grafana dashboard. The project teaches event-driven architecture, exactly-once processing semantics, and stream processing at scale.

## Glossary

- **Pipeline**: The end-to-end system from data ingestion to visualization.
- **Producer**: A component that fetches data from an external source and publishes events to Kafka.
- **Kafka**: The distributed event streaming broker that decouples producers from consumers.
- **Topic**: A named Kafka channel to which events are published and from which consumers read.
- **Stream_Processor**: The Apache Flink or Spark Streaming job that consumes events from Kafka and applies transformations.
- **Event**: A single immutable record published to a Kafka topic, containing a timestamp, source identifier, and payload.
- **Exactly_Once_Semantics**: A processing guarantee ensuring each event is processed and written to storage exactly one time, even in the presence of failures.
- **Table_Store**: The Delta Lake or Apache Iceberg storage layer that persists processed events as versioned, ACID-compliant tables.
- **dbt_Model**: A SQL transformation defined in dbt that reads from the Table_Store and produces an aggregated or enriched table.
- **Grafana**: The visualization layer that queries the Table_Store or dbt models and renders dashboards.
- **Dead_Letter_Queue**: A dedicated Kafka topic that receives events that could not be parsed or processed successfully.
- **Checkpoint**: A Flink or Spark Streaming snapshot of operator state used to recover processing position after a failure.
- **Schema_Registry**: A service that stores and enforces Avro or JSON Schema definitions for Kafka topics.
- **Watermark**: A progress marker in the stream that signals the Stream_Processor that all events up to a given timestamp have been received.

---

## Requirements

### Requirement 1: Data Ingestion

**User Story:** As a data engineer, I want the pipeline to continuously ingest live data from at least one configurable external source, so that the system always has a fresh stream of events to process.

#### Acceptance Criteria

1. THE Producer SHALL support at least one of the following source types: a public REST API (e.g., stock prices), a simulated IoT sensor feed, or a WebSocket stream.
2. WHEN the Producer starts, THE Producer SHALL begin publishing events to the configured Kafka topic within 10 seconds.
3. WHEN the external source returns a valid response, THE Producer SHALL publish one Event per data point to the Kafka topic within 500 ms of receipt.
4. IF the external source is unreachable for more than 30 seconds, THEN THE Producer SHALL log a structured error message and retry the connection using exponential backoff with a maximum interval of 60 seconds.
5. THE Producer SHALL serialize each Event using the schema registered in the Schema_Registry before publishing to Kafka.
6. WHERE multiple source types are configured, THE Producer SHALL publish events from each source to a dedicated Kafka Topic named after the source type.

---

### Requirement 2: Event Schema and Validation

**User Story:** As a data engineer, I want all events to conform to a defined schema, so that downstream consumers can reliably parse and process them.

#### Acceptance Criteria

1. THE Schema_Registry SHALL store a versioned schema for each Kafka Topic used by the Pipeline.
2. WHEN the Producer publishes an Event, THE Schema_Registry SHALL validate the Event against the registered schema before the message is accepted by Kafka.
3. IF an Event fails schema validation, THEN THE Producer SHALL route the Event to the Dead_Letter_Queue and log the validation error with the offending field and value.
4. THE Event schema SHALL include at minimum: `event_id` (UUID), `source` (string), `timestamp` (ISO-8601 UTC), and `payload` (key-value map).
5. WHEN a schema version is updated, THE Schema_Registry SHALL maintain backward compatibility so that consumers using the previous schema version continue to deserialize events without error.

---

### Requirement 3: Stream Processing

**User Story:** As a data engineer, I want the stream processor to transform and aggregate events in real time, so that meaningful metrics are available with low latency.

#### Acceptance Criteria

1. THE Stream_Processor SHALL consume events from the configured Kafka Topic using a consumer group with a committed offset per partition.
2. WHEN the Stream_Processor reads an Event, THE Stream_Processor SHALL parse the payload and apply the configured transformation within 1 second of the event's Kafka ingestion timestamp.
3. THE Stream_Processor SHALL compute a tumbling-window aggregation (e.g., count, sum, or average) over a configurable window size between 10 seconds and 5 minutes.
4. WHEN a Watermark advances past a window boundary, THE Stream_Processor SHALL emit the aggregated result for that window to the Table_Store.
5. THE Stream_Processor SHALL write aggregated results to the Table_Store using Exactly_Once_Semantics.
6. WHEN a Checkpoint is triggered, THE Stream_Processor SHALL save operator state to durable storage within 30 seconds.
7. IF the Stream_Processor fails and restarts, THEN THE Stream_Processor SHALL resume processing from the last successful Checkpoint without duplicating or dropping events.
8. THE Stream_Processor SHALL expose a health endpoint that returns HTTP 200 when the job is running and HTTP 503 when the job is stopped or in an error state.

---

### Requirement 4: Exactly-Once Semantics

**User Story:** As a data engineer, I want the pipeline to guarantee exactly-once processing, so that metrics and aggregations are accurate even after failures.

#### Acceptance Criteria

1. THE Stream_Processor SHALL use transactional writes to the Table_Store so that partial writes are rolled back on failure.
2. WHEN a Checkpoint completes successfully, THE Stream_Processor SHALL commit the corresponding Kafka consumer offsets atomically with the Table_Store write transaction.
3. IF a duplicate Event with the same `event_id` is received within the same processing window, THEN THE Stream_Processor SHALL deduplicate the Event and write only one result to the Table_Store.
4. THE Table_Store SHALL record the `event_id` of each processed Event so that idempotency can be verified by querying the store.
5. FOR ALL Events processed through the Pipeline, the count of records written to the Table_Store SHALL equal the count of unique `event_id` values published to Kafka for the same time window.

---

### Requirement 5: Table Storage (Delta Lake / Iceberg)

**User Story:** As a data engineer, I want processed events and aggregations stored in a versioned, queryable table format, so that I can run historical queries and support time-travel analysis.

#### Acceptance Criteria

1. THE Table_Store SHALL persist raw events in a `raw_events` table and aggregated results in an `aggregations` table.
2. THE Table_Store SHALL support ACID transactions so that concurrent reads and writes do not produce inconsistent results.
3. WHEN a new batch of records is written, THE Table_Store SHALL create a new table version and retain the previous version for at least 7 days.
4. WHEN a time-travel query specifies a past version or timestamp, THE Table_Store SHALL return the data as it existed at that point in time.
5. THE Table_Store SHALL partition the `raw_events` table by `source` and by date (derived from `timestamp`) to enable efficient partition pruning.
6. IF a write transaction is aborted, THEN THE Table_Store SHALL roll back all changes from that transaction and leave the table in its previous consistent state.

---

### Requirement 6: dbt Transformations

**User Story:** As a data engineer, I want dbt models to produce clean, aggregated datasets from the raw table store, so that Grafana dashboards query well-structured data.

#### Acceptance Criteria

1. THE dbt_Model SHALL read from the `raw_events` table in the Table_Store and produce at least one aggregated output table (e.g., per-source event counts per minute).
2. WHEN a dbt run completes, THE dbt_Model SHALL write results to a dedicated schema in the Table_Store without modifying the `raw_events` table.
3. THE dbt_Model SHALL include data tests that assert: no null `event_id` values, no duplicate `event_id` values within a partition, and `timestamp` values within a valid range.
4. IF a dbt data test fails, THEN THE dbt_Model run SHALL exit with a non-zero status code and log the failing test name and row count.
5. THE dbt_Model SHALL be runnable on a configurable schedule (e.g., every 5 minutes) via a scheduler such as Airflow or cron.

---

### Requirement 7: Grafana Dashboard

**User Story:** As a data engineer, I want a Grafana dashboard that visualizes real-time and historical pipeline metrics, so that I can demonstrate the pipeline's behavior and health at a glance.

#### Acceptance Criteria

1. THE Grafana dashboard SHALL display at least the following panels: events ingested per minute, processing latency (p50 and p95), aggregated metric values per source, and Dead_Letter_Queue event count.
2. WHEN new data is written to the Table_Store, THE Grafana dashboard SHALL refresh and display updated values within 60 seconds.
3. THE Grafana dashboard SHALL be provisioned as code (JSON or YAML) so that it can be version-controlled and reproduced in a new environment without manual configuration.
4. WHERE a time range selector is used, THE Grafana dashboard SHALL query the Table_Store for the selected range and update all panels accordingly.
5. THE Grafana dashboard SHALL include an alert rule that triggers when the Dead_Letter_Queue event count exceeds 10 events within a 5-minute window.

---

### Requirement 8: Dead Letter Queue Handling

**User Story:** As a data engineer, I want malformed or unprocessable events captured in a dead letter queue, so that no data is silently lost and failures can be investigated.

#### Acceptance Criteria

1. THE Dead_Letter_Queue SHALL be a dedicated Kafka Topic named `<source-topic>.dlq`.
2. WHEN an Event is routed to the Dead_Letter_Queue, THE Pipeline SHALL attach metadata including: original topic, partition, offset, error type, and error message.
3. THE Dead_Letter_Queue SHALL retain messages for at least 72 hours.
4. WHEN the Dead_Letter_Queue receives an Event, THE Grafana dashboard panel for DLQ count SHALL reflect the new count within the next dashboard refresh cycle.
5. THE Pipeline SHALL expose a reprocessing utility that reads events from the Dead_Letter_Queue, corrects the schema if possible, and republishes them to the original Kafka Topic.

---

### Requirement 9: Observability and Logging

**User Story:** As a data engineer, I want structured logs and metrics from every pipeline component, so that I can diagnose failures and monitor performance.

#### Acceptance Criteria

1. THE Pipeline SHALL emit structured JSON logs from the Producer, Stream_Processor, and dbt_Model runs, including at minimum: `timestamp`, `component`, `level`, `message`, and `event_id` where applicable.
2. THE Stream_Processor SHALL expose Prometheus-compatible metrics including: records consumed per second, records written per second, checkpoint duration, and processing lag per Kafka partition.
3. WHEN a component encounters an unhandled exception, THE component SHALL log the full stack trace at ERROR level and continue operation where possible.
4. THE Pipeline SHALL include a `docker-compose.yml` that starts all components (Kafka, Schema Registry, Stream Processor, Table Store, Grafana) with a single command for local development.

---

### Requirement 10: Local Development and Reproducibility

**User Story:** As a data engineer, I want the entire pipeline runnable locally with a single command, so that I can develop, test, and demonstrate the project without cloud infrastructure.

#### Acceptance Criteria

1. THE Pipeline SHALL provide a `docker-compose.yml` that starts all required services: Kafka, Zookeeper (or KRaft), Schema_Registry, Stream_Processor, Table_Store, dbt runner, and Grafana.
2. WHEN `docker compose up` is executed in the project root, THE Pipeline SHALL have all services healthy and producing data within 3 minutes on a machine with at least 8 GB of RAM and 4 CPU cores.
3. THE Pipeline SHALL include a seed script that generates synthetic events and publishes them to Kafka so that the dashboard is populated without requiring a live external API key.
4. THE Pipeline SHALL include a `README.md` with step-by-step setup instructions, architecture diagram description, and an explanation of the exactly-once semantics implementation.
5. IF a required environment variable is missing at startup, THEN THE affected component SHALL log a descriptive error message identifying the missing variable and exit with a non-zero status code.
