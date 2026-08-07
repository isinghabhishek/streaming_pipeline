package com.pipeline.flink.operator;

import com.pipeline.flink.health.MetricsRegistry;
import com.pipeline.flink.model.DLQEvent;
import com.pipeline.flink.model.Event;
import com.pipeline.flink.serde.AvroEventDeserializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flink {@link ProcessFunction} that deserializes raw Avro bytes into {@link Event} objects.
 *
 * <p>This operator is the canonical parse-and-route step in the Flink job topology:
 * <ul>
 *   <li><b>Success path:</b> emits a parsed {@link Event} to the main output.</li>
 *   <li><b>Failure path:</b> constructs a {@link DLQEvent} with full error metadata and emits it
 *       to the {@link #DLQ_TAG} side output; increments both the Flink {@code parse_errors}
 *       counter metric and the in-process {@link MetricsRegistry#parseErrors} counter so the
 *       Prometheus scrape endpoint reflects failures in real time.</li>
 * </ul>
 *
 * <p>Using {@link ProcessFunction} (rather than a plain {@code FlatMapFunction} or
 * {@code MapFunction}) is required because Flink's side-output API is only available
 * through the {@link Context} provided by {@code ProcessFunction.processElement}.
 *
 * <p>Typical usage in the job topology:
 * <pre>{@code
 *   SingleOutputStreamOperator<Event> parsedStream = rawStream
 *       .process(new ParseMap("my-topic"))
 *       .name("parse-avro");
 *
 *   DataStream<DLQEvent> dlqStream = parsedStream.getSideOutput(ParseMap.DLQ_TAG);
 * }</pre>
 *
 * <p>Requirements: 3.2
 */
public class ParseMap extends ProcessFunction<byte[], Event> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(ParseMap.class);

    /**
     * Side-output tag for DLQ events — shared between this operator and the job topology.
     * Callers retrieve the DLQ stream via {@code parsedStream.getSideOutput(ParseMap.DLQ_TAG)}.
     */
    public static final OutputTag<DLQEvent> DLQ_TAG =
            new OutputTag<DLQEvent>("dlq-parse-errors") {};

    /** Source topic name attached to every DLQ envelope (helps downstream routing). */
    private final String sourceTopic;

    /**
     * Flink-managed parse_errors counter registered with the metrics subsystem.
     * Exposed via the Prometheus reporter configured in flink-conf.yaml.
     */
    private transient Counter parseErrorsCounter;

    /**
     * Constructs a {@code ParseMap} for the given source topic.
     *
     * @param sourceTopic Kafka topic name consumed by this job; embedded in every DLQ envelope
     *                    as {@code original_topic}.
     */
    public ParseMap(String sourceTopic) {
        this.sourceTopic = sourceTopic;
    }

    @Override
    public void open(Configuration parameters) {
        parseErrorsCounter = getRuntimeContext()
                .getMetricGroup()
                .counter("parse_errors");
    }

    /**
     * Processes a single raw byte array from Kafka.
     *
     * <p>On success the deserialized {@link Event} is forwarded to the main collector.
     * On any exception the raw bytes are wrapped in a {@link DLQEvent} and routed to
     * {@link #DLQ_TAG}; the {@code parse_errors} counter is incremented.
     *
     * @param rawBytes  raw Kafka message bytes (Confluent wire format or plain Avro)
     * @param ctx       Flink ProcessFunction context (provides side-output access)
     * @param out       main output collector
     */
    @Override
    public void processElement(byte[] rawBytes,
                               Context ctx,
                               Collector<Event> out) {
        try {
            Event event = AvroEventDeserializer.parse(rawBytes);
            out.collect(event);
        } catch (Exception e) {
            // Increment both the Flink-managed metric (scraped by Prometheus reporter)
            // and the in-process MetricsRegistry counter (served by the health endpoint).
            parseErrorsCounter.inc();
            MetricsRegistry.incParseErrors();

            String errorMsg = e.getMessage() != null ? e.getMessage() : e.toString();

            LOG.error("{\"component\":\"ParseMap\","
                    + "\"level\":\"ERROR\","
                    + "\"message\":\"Avro parse failure — routing to DLQ\","
                    + "\"error_type\":\"{}\","
                    + "\"error_message\":\"{}\","
                    + "\"source_topic\":\"{}\"}",
                    e.getClass().getSimpleName(), errorMsg, sourceTopic);

            DLQEvent dlqEvent = new DLQEvent(
                    sourceTopic,
                    /* original_partition — unavailable in FlatMap context */ -1,
                    /* original_offset    — unavailable in FlatMap context */ -1L,
                    e.getClass().getSimpleName(),
                    errorMsg,
                    rawBytes
            );

            ctx.output(DLQ_TAG, dlqEvent);
        }
    }
}
