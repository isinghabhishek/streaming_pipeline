package com.pipeline.flink.operator;

import com.pipeline.flink.model.Event;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Keyed process function that deduplicates events by {@code event_id}.
 *
 * <p>The stream must be keyed on {@code event_id} before this operator.
 * A boolean ValueState records whether the key has been seen. State TTL is
 * set to the window size + 60 s so that duplicates arriving within a processing
 * window are suppressed, while memory is not leaked for unique keys seen long ago.
 *
 * <p>This satisfies Requirements 4.3 and 4.4 (deduplicate within processing window,
 * record event_id for idempotency verification).
 */
public class EventDeduplicator extends KeyedProcessFunction<String, Event, Event> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(EventDeduplicator.class);

    /** TTL for the seen-state: windowSizeSeconds + 60 s grace period. */
    private final long stateTtlSeconds;

    private transient ValueState<Boolean> seenState;

    /**
     * @param windowSizeSeconds  configured tumbling-window size in seconds;
     *                           state TTL = windowSizeSeconds + 60 s
     */
    public EventDeduplicator(long windowSizeSeconds) {
        this.stateTtlSeconds = windowSizeSeconds + 60L;
    }

    @Override
    public void open(Configuration parameters) {
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.seconds(stateTtlSeconds))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();

        ValueStateDescriptor<Boolean> descriptor =
                new ValueStateDescriptor<>("event-id-seen", Boolean.class);
        descriptor.enableTimeToLive(ttlConfig);

        seenState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(Event event,
                               Context ctx,
                               Collector<Event> out) throws Exception {
        Boolean seen = seenState.value();
        if (seen != null && seen) {
            LOG.debug("{\"component\":\"EventDeduplicator\",\"level\":\"DEBUG\","
                    + "\"message\":\"Duplicate event suppressed\","
                    + "\"event_id\":\"{}\"}",
                    event.getEventId());
            // drop duplicate — do not emit
            return;
        }
        seenState.update(Boolean.TRUE);
        out.collect(event);
    }
}
