package com.pipeline.flink;

import com.pipeline.flink.config.CheckpointConfig;
import com.pipeline.flink.health.HealthServer;
import com.pipeline.flink.health.MetricsRegistry;
import com.pipeline.flink.model.AggregationResult;
import com.pipeline.flink.model.DLQEvent;
import com.pipeline.flink.model.Event;
import com.pipeline.flink.operator.DlqRoutingProcessFunction;
import com.pipeline.flink.operator.EventDeduplicator;
import com.pipeline.flink.operator.WindowAggregateFunction;
import com.pipeline.flink.operator.WindowResultFunction;
import com.pipeline.flink.sink.DeltaSinkFactory;
import com.pipeline.flink.source.KafkaSourceConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point for the Flink stream processor.
 *
 * <p><b>Job topology:</b>
 * <pre>
 * KafkaSource (byte[])
 *   → WatermarkStrategy (ingestion-time placeholder)
 *   → DlqRoutingProcessFunction          ← parse Avro; failures → DLQ side output
 *   → assignTimestampsAndWatermarks      ← event-time from Event.timestamp
 *   → keyBy(event_id)
 *   → EventDeduplicator                  ← drop duplicate event_ids (keyed state)
 *   → keyBy(source)
 *   → TumblingEventTimeWindow
 *   → aggregate(WindowAggregateFunction, WindowResultFunction)
 *   → map(toAggregationsRow)
 *   → DeltaSink (aggregations table)     ← exactly-once via 2PC
 *
 * DlqRoutingProcessFunction (main output: Event)
 *   → map(toRawEventsRow)
 *   → DeltaSink (raw_events table)       ← exactly-once via 2PC
 *
 * DlqRoutingProcessFunction (side output: DLQEvent)
 *   → (DLQ sink — Kafka topic, future work; currently logged)
 * </pre>
 *
 * <p>Configuration is read entirely from environment variables so the job
 * is container-friendly and matches the Docker Compose setup.
 *
 * <p>Required env vars:
 * <ul>
 *   <li>{@code KAFKA_BOOTSTRAP_SERVERS}</li>
 *   <li>{@code KAFKA_TOPIC}</li>
 *   <li>{@code DELTA_TABLE_PATH}  — root path; {@code /raw_events} and
 *       {@code /aggregations} sub-paths are appended automatically</li>
 * </ul>
 *
 * <p>Optional env vars (with defaults):
 * <ul>
 *   <li>{@code KAFKA_CONSUMER_GROUP}    — default {@code flink-stream-processor}</li>
 *   <li>{@code WINDOW_SIZE_SECONDS}     — default {@code 60}, range 10–300</li>
 *   <li>{@code CHECKPOINT_INTERVAL_MS}  — default {@code 10000}</li>
 *   <li>{@code CHECKPOINT_DIR}          — default {@code file:///data/checkpoints}</li>
 *   <li>{@code HEALTH_PORT}             — default {@code 8081}</li>
 * </ul>
 */
public class StreamingJob {

    private static final Logger LOG = LoggerFactory.getLogger(StreamingJob.class);

    // -------------------------------------------------------------------------
    // Defaults
    // -------------------------------------------------------------------------

    private static final String DEFAULT_CONSUMER_GROUP = "flink-stream-processor";
    private static final long   DEFAULT_WINDOW_SECONDS = 60L;
    private static final String DEFAULT_CHECKPOINT_DIR = "file:///data/checkpoints";
    private static final int    DEFAULT_HEALTH_PORT    = 8081;
    private static final long   MIN_WINDOW_SECONDS     = 10L;
    private static final long   MAX_WINDOW_SECONDS     = 300L;

    // -------------------------------------------------------------------------
    // Main
    // -------------------------------------------------------------------------

    public static void main(String[] args) throws Exception {

        // --- Read and validate env vars --------------------------------------
        String bootstrapServers = requireEnv("KAFKA_BOOTSTRAP_SERVERS");
        String kafkaTopic       = requireEnv("KAFKA_TOPIC");
        String deltaTablePath   = requireEnv("DELTA_TABLE_PATH");

        String consumerGroup    = envOrDefault("KAFKA_CONSUMER_GROUP", DEFAULT_CONSUMER_GROUP);
        long   windowSeconds    = clampWindow(envLong("WINDOW_SIZE_SECONDS", DEFAULT_WINDOW_SECONDS));
        long   checkpointMs     = CheckpointConfig.intervalMsFromEnv();
        String checkpointDir    = envOrDefault("CHECKPOINT_DIR", DEFAULT_CHECKPOINT_DIR);
        int    healthPort       = (int) envLong("HEALTH_PORT", DEFAULT_HEALTH_PORT);

        // --- Health server ---------------------------------------------------
        HealthServer healthServer = new HealthServer(healthPort);
        healthServer.start();

        // --- Flink environment -----------------------------------------------
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CheckpointConfig.apply(env, checkpointDir, checkpointMs);

        // --- Kafka source ----------------------------------------------------
        KafkaSource<byte[]> kafkaSource = KafkaSourceConfig.buildKafkaSource(
                bootstrapServers, kafkaTopic, consumerGroup);

        WatermarkStrategy<byte[]> rawWatermarks = KafkaSourceConfig.buildRawWatermarkStrategy();

        DataStream<byte[]> rawStream = env
                .fromSource(kafkaSource, rawWatermarks, "kafka-source");

        // --- Parse + DLQ routing --------------------------------------------
        SingleOutputStreamOperator<Event> parsedStream = rawStream
                .process(new DlqRoutingProcessFunction(kafkaTopic))
                .name("parse-avro-dlq-route");

        // Extract DLQ side output
        DataStream<DLQEvent> dlqStream =
                parsedStream.getSideOutput(DlqRoutingProcessFunction.DLQ_TAG);

        // Increment DLQ counter for each routed event
        dlqStream.map(dlq -> {
            MetricsRegistry.incDlqEvents();
            LOG.warn("{\"component\":\"StreamingJob\",\"level\":\"WARN\","
                    + "\"message\":\"DLQ event routed\","
                    + "\"original_topic\":\"{}\","
                    + "\"error_type\":\"{}\","
                    + "\"error_message\":\"{}\"}",
                    dlq.getOriginalTopic(), dlq.getErrorType(), dlq.getErrorMessage());
            return dlq;
        }).name("dlq-logger");
        // TODO Task 10: wire DLQ stream to Kafka DLQ topic sink

        // Increment consumed counter on the parsed stream
        DataStream<Event> eventStream = parsedStream.map(event -> {
            MetricsRegistry.incRecordsConsumed();
            return event;
        }).name("count-consumed");

        // --- Assign event-time watermarks on parsed stream -------------------
        DataStream<Event> timedStream = eventStream
                .assignTimestampsAndWatermarks(KafkaSourceConfig.buildEventWatermarkStrategy())
                .name("event-time-watermarks");

        // --- Deduplication (keyed by event_id) --------------------------------
        DataStream<Event> dedupedStream = timedStream
                .keyBy(Event::getEventId)
                .process(new EventDeduplicator(windowSeconds))
                .name("event-dedup");

        // --- Raw events → Delta Lake -----------------------------------------
        DataStream<RowData> rawRows = dedupedStream
                .map(DeltaSinkFactory::toRawEventsRow)
                .name("to-raw-events-row");

        rawRows
                .sinkTo(DeltaSinkFactory.rawEventsSink(deltaTablePath + "/raw_events"))
                .name("delta-sink-raw-events");

        // Update written counter via a pass-through map before sinking
        dedupedStream.map(event -> {
            MetricsRegistry.incRecordsWritten();
            return event;
        }).name("count-written-raw");

        // --- Windowed aggregation (keyed by source) --------------------------
        DataStream<AggregationResult> aggregations = dedupedStream
                .keyBy(Event::getSource)
                .window(TumblingEventTimeWindows.of(Time.seconds(windowSeconds)))
                .aggregate(new WindowAggregateFunction(), new WindowResultFunction())
                .name("tumbling-window-agg");

        // --- Aggregations → Delta Lake ----------------------------------------
        aggregations
                .map(DeltaSinkFactory::toAggregationsRow)
                .name("to-aggregations-row")
                .sinkTo(DeltaSinkFactory.aggregationsSink(deltaTablePath + "/aggregations"))
                .name("delta-sink-aggregations");

        // --- Execute ----------------------------------------------------------
        healthServer.setRunning();
        LOG.info("{\"component\":\"StreamingJob\",\"level\":\"INFO\","
                + "\"message\":\"Submitting Flink job\","
                + "\"topic\":\"{}\","
                + "\"window_seconds\":{},\"checkpoint_ms\":{}}",
                kafkaTopic, windowSeconds, checkpointMs);

        try {
            env.execute("real-time-streaming-pipeline");
        } catch (Exception e) {
            healthServer.setError();
            LOG.error("{\"component\":\"StreamingJob\",\"level\":\"ERROR\","
                    + "\"message\":\"Job execution failed\","
                    + "\"error_type\":\"{}\","
                    + "\"error_message\":\"{}\"}",
                    e.getClass().getSimpleName(), e.getMessage());
            throw e;
        } finally {
            healthServer.setStopped();
            healthServer.stop();
        }
    }

    // -------------------------------------------------------------------------
    // Env-var helpers
    // -------------------------------------------------------------------------

    /**
     * Returns the value of the given env var, or exits with code 1 if absent/empty.
     * Satisfies Requirement 10.5 (descriptive error + non-zero exit on missing var).
     */
    static String requireEnv(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            String msg = String.format(
                    "{\"component\":\"StreamingJob\",\"level\":\"ERROR\","
                    + "\"message\":\"Required environment variable is not set\","
                    + "\"variable\":\"%s\"}", name);
            LOG.error(msg);
            System.err.println(msg);
            System.exit(1);
        }
        return value;
    }

    static String envOrDefault(String name, String defaultValue) {
        String v = System.getenv(name);
        return (v == null || v.isBlank()) ? defaultValue : v.trim();
    }

    static long envLong(String name, long defaultValue) {
        String v = System.getenv(name);
        if (v == null || v.isBlank()) return defaultValue;
        try {
            return Long.parseLong(v.trim());
        } catch (NumberFormatException e) {
            LOG.warn("Invalid value for env var {}='{}'; using default {}", name, v, defaultValue);
            return defaultValue;
        }
    }

    static long clampWindow(long seconds) {
        if (seconds < MIN_WINDOW_SECONDS) {
            LOG.warn("WINDOW_SIZE_SECONDS {} below minimum {}; clamping",
                    seconds, MIN_WINDOW_SECONDS);
            return MIN_WINDOW_SECONDS;
        }
        if (seconds > MAX_WINDOW_SECONDS) {
            LOG.warn("WINDOW_SIZE_SECONDS {} above maximum {}; clamping",
                    seconds, MAX_WINDOW_SECONDS);
            return MAX_WINDOW_SECONDS;
        }
        return seconds;
    }
}
