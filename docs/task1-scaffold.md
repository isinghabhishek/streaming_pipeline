# Task 1: Scaffold Project Structure and Docker Compose Environment

**Status:** Completed  
**Requirements:** 9.4, 10.1, 10.2

---

## What Was Implemented

### Directory Layout

```
real-time-streaming-pipeline/
├── producer/               # Python producer service
├── flink-job/              # Apache Flink stream processor (Java/Scala)
├── storage/                # Delta Lake init scripts
├── dbt/                    # dbt transformation models
├── grafana/
│   └── provisioning/
│       ├── datasources/    # Grafana datasource YAML configs
│       └── dashboards/     # Grafana dashboard JSON configs
├── scripts/                # Utility and init scripts
├── schemas/                # Avro schema definitions
├── docs/                   # Project documentation (this folder)
├── docker-compose.yml
└── .env.example
```

---

## Files Created

### `docker-compose.yml`

Defines 8 services, all with health checks so `docker compose up --wait` confirms readiness:

| Service | Image | Port(s) | Health Check |
|---|---|---|---|
| `kafka` | bitnami/kafka:3.6 (KRaft mode) | 9092 | `kafka-topics.sh --list` |
| `schema-registry` | confluentinc/cp-schema-registry:7.6.0 | 8081 | `GET /subjects` |
| `schema-init` | python:3.11-slim (init container) | — | Runs once, `restart: on-failure` |
| `flink-jobmanager` | apache/flink:1.18 | 8082 | `GET /overview` |
| `flink-taskmanager` | apache/flink:1.18 | — | Depends on jobmanager healthy |
| `spark-thrift` | bitnami/spark:3.5 | 10000, 4040 | `nc -z localhost 10000` |
| `dbt-runner` | python:3.11-slim (cron) | — | Depends on spark-thrift healthy |
| `grafana` | grafana/grafana:latest | 3000 | `GET /api/health` |

Key design decisions:
- Kafka runs in **KRaft mode** — no Zookeeper dependency
- Flink checkpoints are persisted to a named Docker volume (`flink_checkpoints`)
- Delta Lake data is persisted to a named Docker volume (`delta_data`)
- All services read from `.env` via `env_file: .env`
- DLQ topic retention is set to 72 hours (`KAFKA_CFG_LOG_RETENTION_MS: 259200000`)

---

### `.env.example`

Documents all required environment variables grouped by component:

| Group | Variables |
|---|---|
| Kafka | `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_CLUSTER_ID` |
| Schema Registry | `SCHEMA_REGISTRY_URL` |
| Producer | `SOURCE_TYPE`, `SOURCE_URL`, `POLL_INTERVAL_MS` |
| Flink | `KAFKA_TOPIC`, `WINDOW_SIZE_SECONDS`, `CHECKPOINT_INTERVAL_MS`, `DELTA_TABLE_PATH`, `FLINK_TASK_SLOTS` |
| Spark / Delta Lake | `SPARK_MASTER`, `DELTA_STORAGE_PATH` |
| dbt | `DBT_SPARK_HOST`, `DBT_SPARK_PORT`, `DBT_SCHEMA`, `DBT_CRON_SCHEDULE` |
| Grafana | `GF_SECURITY_ADMIN_USER`, `GF_SECURITY_ADMIN_PASSWORD`, `GF_SERVER_ROOT_URL` |

Copy to `.env` and fill in values before running `docker compose up`.

---

### `schemas/event.avsc`

Avro schema for the main pipeline event:

```json
{
  "type": "record",
  "name": "Event",
  "namespace": "com.pipeline",
  "fields": [
    { "name": "event_id",  "type": "string" },
    { "name": "source",    "type": "string" },
    { "name": "timestamp", "type": "string" },
    { "name": "payload",   "type": { "type": "map", "values": "string" } }
  ]
}
```

Registered in Schema Registry under subject: `events-value`

---

### `schemas/dlq_event.avsc`

Avro schema for Dead Letter Queue messages:

```json
{
  "type": "record",
  "name": "DLQEvent",
  "namespace": "com.pipeline",
  "fields": [
    { "name": "original_topic",     "type": "string" },
    { "name": "original_partition", "type": "int" },
    { "name": "original_offset",    "type": "long" },
    { "name": "error_type",         "type": "string" },
    { "name": "error_message",      "type": "string" },
    { "name": "raw_bytes",          "type": "bytes" }
  ]
}
```

Registered in Schema Registry under subject: `events.dlq-value`

---

### `scripts/register_schemas.py`

Init script run by the `schema-init` container at startup:

1. Polls Schema Registry until it responds (up to 10 retries, 3 s apart)
2. For each schema (`event.avsc`, `dlq_event.avsc`):
   - Checks backward compatibility against the latest registered version via `POST /compatibility/subjects/{subject}/versions/latest`
   - POSTs the schema to `POST /subjects/{subject}/versions`
   - Logs the assigned schema ID on success
3. Exits with code `0` on full success, `1` if any registration fails

---

## How to Run

```bash
# 1. Copy and configure environment
cp .env.example .env

# 2. Start all services and wait for health checks
docker compose up --wait

# 3. Verify schemas were registered
curl http://localhost:8081/subjects
```

Expected output from step 3:
```json
["events-value", "events.dlq-value"]
```

---

## Next Task

**Task 2: Define Avro schemas and Schema Registry integration** — write property tests for Event serialization round-trip and schema backward compatibility.
