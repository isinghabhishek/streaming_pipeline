package com.pipeline.flink.operator;

import com.pipeline.flink.model.DLQEvent;
import com.pipeline.flink.model.Event;
import com.pipeline.flink.serde.AvroEventDeserializer;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ProcessFunction that deserializes raw Avro bytes into {@link Event} objects.
 *
 * <p>On parse failure the failing bytes are wrapped in a {@link DLQEvent} and
 * emitted to the {@link #DLQ_TAG} side output, while the {@code parse_errors}
 * Flink counter metric is incremented. The main output receives only successfully
 * parsed events.
 *
 * <p>Using a {@link ProcessFunction} (rather than a plain {@code FlatMapFunction})
 * is required because Flink's side-output API is only available via the
 * {@link org.apache.flink.util.Collector} provided by ProcessFunction context.
 */
public class DlqRoutingProcessFunction extends ProcessFunction<byte[], Event> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(DlqRoutingProcessFunction.class);

    /** Side-output tag for DLQ events — shared with the job topology. */
    public static final OutputTag<DLQEvent> DLQ_TAG =
            new OutputTag<DLQEvent>("dlq-parse-errors") {};

    private final String sourceTopic;
    private transient Counter parseErrorsCounter;

    public DlqRoutingProcessFunction(String sourceTopic) {
        this.sourceTopic = sourceTopic;
    }

    @Override
    public void open(Configuration parameters) {
        parseErrorsCounter = getRuntimeContext()
                .getMetricGroup()
                .counter("parse_errors");
    }

    @Override
    public void processElement(byte[] rawBytes,
                               Context ctx,
                               Collector<Event> out) throws Exception {
        try {
            Event event = AvroEventDeserializer.parse(rawBytes);
            out.collect(event);
        } catch (Exception e) {
            parseErrorsCounter.inc();

            String errorMsg = e.getMessage() != null ? e.getMessage() : e.toString();

            LOG.error("{\"component\":\"DlqRoutingProcessFunction\","
                    + "\"level\":\"ERROR\","
                    + "\"message\":\"Avro parse failure — routing to DLQ\","
                    + "\"error_type\":\"{}\","
                    + "\"error_message\":\"{}\","
                    + "\"source_topic\":\"{}\"}",
                    e.getClass().getSimpleName(), errorMsg, sourceTopic);

            DLQEvent dlqEvent = new DLQEvent(
                    sourceTopic,
                    /* partition */ -1,
                    /* offset    */ -1L,
                    e.getClass().getSimpleName(),
                    errorMsg,
                    rawBytes
            );

            ctx.output(DLQ_TAG, dlqEvent);
        }
    }
}
