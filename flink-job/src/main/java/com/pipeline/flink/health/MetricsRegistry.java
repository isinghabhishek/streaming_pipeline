package com.pipeline.flink.health;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Simple in-process metrics registry exposing Prometheus text-format counters
 * and gauges for the Flink streaming job.
 *
 * <p>These metrics are served by {@link HealthServer} at {@code GET /metrics}
 * and can be scraped by a Prometheus instance or Grafana agent.
 *
 * <p>Metrics exposed (Requirement 9.2):
 * <ul>
 *   <li>{@code pipeline_records_consumed_total}   — events read from Kafka</li>
 *   <li>{@code pipeline_records_written_total}    — records written to Delta Lake</li>
 *   <li>{@code pipeline_parse_errors_total}       — Avro parse failures</li>
 *   <li>{@code pipeline_dlq_events_total}         — events sent to the DLQ</li>
 *   <li>{@code pipeline_checkpoint_duration_ms}   — last checkpoint duration (gauge)</li>
 *   <li>{@code pipeline_processing_lag_ms}        — last recorded processing lag per partition (gauge)</li>
 * </ul>
 */
public final class MetricsRegistry {

    private MetricsRegistry() {}

    // -------------------------------------------------------------------------
    // Counters (thread-safe, monotonically increasing)
    // -------------------------------------------------------------------------

    public static final LongAdder recordsConsumed   = new LongAdder();
    public static final LongAdder recordsWritten    = new LongAdder();
    public static final LongAdder parseErrors       = new LongAdder();
    public static final LongAdder dlqEvents         = new LongAdder();

    // -------------------------------------------------------------------------
    // Gauges (last-written value; AtomicLong for visibility)
    // -------------------------------------------------------------------------

    public static final AtomicLong checkpointDurationMs = new AtomicLong(0L);
    public static final AtomicLong processingLagMs      = new AtomicLong(0L);

    // -------------------------------------------------------------------------
    // Convenience increment helpers
    // -------------------------------------------------------------------------

    public static void incRecordsConsumed()   { recordsConsumed.increment(); }
    public static void incRecordsWritten()    { recordsWritten.increment(); }
    public static void incParseErrors()       { parseErrors.increment(); }
    public static void incDlqEvents()         { dlqEvents.increment(); }

    public static void setCheckpointDuration(long ms) { checkpointDurationMs.set(ms); }
    public static void setProcessingLag(long ms)      { processingLagMs.set(ms); }

    // -------------------------------------------------------------------------
    // Prometheus text export
    // -------------------------------------------------------------------------

    /**
     * Renders all metrics in Prometheus text format (exposition format 0.0.4).
     */
    public static String toPrometheusText() {
        StringBuilder sb = new StringBuilder(512);

        appendCounter(sb, "pipeline_records_consumed_total",
                "Total Kafka records consumed by the Flink job",
                recordsConsumed.sum());

        appendCounter(sb, "pipeline_records_written_total",
                "Total records written to Delta Lake",
                recordsWritten.sum());

        appendCounter(sb, "pipeline_parse_errors_total",
                "Total Avro parse errors (events routed to DLQ)",
                parseErrors.sum());

        appendCounter(sb, "pipeline_dlq_events_total",
                "Total events sent to the Dead Letter Queue",
                dlqEvents.sum());

        appendGauge(sb, "pipeline_checkpoint_duration_ms",
                "Duration of the last successful Flink checkpoint in milliseconds",
                checkpointDurationMs.get());

        appendGauge(sb, "pipeline_processing_lag_ms",
                "Processing lag (event-time vs wall-clock) in milliseconds",
                processingLagMs.get());

        return sb.toString();
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static void appendCounter(StringBuilder sb, String name, String help, long value) {
        sb.append("# HELP ").append(name).append(' ').append(help).append('\n');
        sb.append("# TYPE ").append(name).append(" counter\n");
        sb.append(name).append(' ').append(value).append('\n');
    }

    private static void appendGauge(StringBuilder sb, String name, String help, long value) {
        sb.append("# HELP ").append(name).append(' ').append(help).append('\n');
        sb.append("# TYPE ").append(name).append(" gauge\n");
        sb.append(name).append(' ').append(value).append('\n');
    }
}
