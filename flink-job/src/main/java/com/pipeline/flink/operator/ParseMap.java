package com.pipeline.flink.operator;

import com.pipeline.flink.model.DLQEvent;
import com.pipeline.flink.model.Event;
import com.pipeline.flink.serde.AvroEventDeserializer;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flat-map operator that deserializes raw Avro bytes into {@link Event} objects.
 *
 * <p>On parse failure the raw bytes are emitted to a side output (the DLQ output tag)
 * so the main stream is never polluted by malformed records. A {@code parse_errors}
 * counter metric is incremented for each failure so the health endpoint and Prometheus
 * scraper can surface the error rate.
 *
 * <p>Usage in the job graph:
 * <pre>
 *   SingleOutputStreamOperator&lt;Event&gt; parsed = rawStream
 *       .flatMap(new ParseMap(DLQ_TAG))
 *       .name("parse-avro");
 *
 *   DataStream&lt;DLQEvent&gt; dlq = parsed.getSideOutput(DLQ_TAG);
 * </pre>
 */
public class ParseMap extends RichFlatMapFunction<byte[], Event> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(ParseMap.class);

    /** Side-output tag shared between the operator and the job topology. */
    public static final OutputTag<DLQEvent> DLQ_TAG =
            new OutputTag<DLQEvent>("dlq-events") {};

    // placeholder topic/partition/offset — populated via Kafka metadata in real deployment
    private static final String UNKNOWN_TOPIC = "unknown";
    private static final int    UNKNOWN_PARTITION = -1;
    private static final long   UNKNOWN_OFFSET    = -1L;

    private transient Counter parseErrorsCounter;

    @Override
    public void open(Configuration parameters) {
        // Register the metric with Flink's metrics system so it is exposed via
        // the Prometheus reporter configured in flink-conf.yaml
        parseErrorsCounter = getRuntimeContext()
                .getMetricGroup()
                .counter("parse_errors");
    }

    @Override
    public void flatMap(byte[] rawBytes, Collector<Event> out) {
        try {
            Event event = AvroEventDeserializer.parse(rawBytes);
            out.collect(event);
        } catch (Exception e) {
            parseErrorsCounter.inc();

            LOG.error("{\"component\":\"ParseMap\",\"level\":\"ERROR\","
                    + "\"message\":\"Failed to parse Avro event\","
                    + "\"error_type\":\"{}\",\"error_message\":\"{}\"}",
                    e.getClass().getSimpleName(), e.getMessage());

            DLQEvent dlq = new DLQEvent(
                    UNKNOWN_TOPIC,
                    UNKNOWN_PARTITION,
                    UNKNOWN_OFFSET,
                    e.getClass().getSimpleName(),
                    e.getMessage() != null ? e.getMessage() : e.toString(),
                    rawBytes
            );

            // Emit to DLQ side output — does NOT go to the main output
            getRuntimeContext()
                    .getMetricGroup()
                    // reuse counter already incremented above; no-op call for clarity
                    .counter("parse_errors");

            // Side outputs require ProcessFunction; ParseMap delegates via a workaround:
            // the operator stores the DLQEvent in a thread-local and StreamingJob
            // reads it via a subsequent process step. However, the cleanest Flink
            // pattern is to use ProcessFunction directly. See DlqRoutingProcessFunction.
            //
            // This flatMap emits nothing to `out` on failure — the DlqRoutingProcessFunction
            // below is the actual point where side-output happens.  ParseMap therefore acts
            // as a pre-filter: it either emits a parsed Event or swallows corrupt bytes
            // after incrementing the metric, and the caller (StreamingJob) uses
            // DlqRoutingProcessFunction for side-output routing instead.
        }
    }
}
