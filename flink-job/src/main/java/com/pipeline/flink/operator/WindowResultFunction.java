package com.pipeline.flink.operator;

import com.pipeline.flink.model.AggregationResult;
import com.pipeline.flink.model.Event;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;

/**
 * {@link ProcessWindowFunction} companion to {@link WindowAggregateFunction}.
 *
 * <p>Flink calls {@link #process} once per window once the watermark passes the
 * window end. The partial aggregation result passed in already has count/sum/avg
 * set; this function enriches it with the window timestamps.
 *
 * <p>Combined with {@link WindowAggregateFunction} via
 * {@code .aggregate(aggFn, windowFn)} for incremental aggregation with
 * full window context — the most efficient pattern in Flink 1.18.
 */
public class WindowResultFunction
        extends ProcessWindowFunction<AggregationResult, AggregationResult, String, TimeWindow> {

    private static final long serialVersionUID = 1L;

    @Override
    public void process(String key,
                        Context context,
                        Iterable<AggregationResult> partials,
                        Collector<AggregationResult> out) {

        // There is exactly one partial result per window because we combine
        // with WindowAggregateFunction incrementally
        AggregationResult partial = partials.iterator().next();

        TimeWindow window = context.window();
        partial.setWindowStart(Instant.ofEpochMilli(window.getStart()));
        partial.setWindowEnd(Instant.ofEpochMilli(window.getEnd()));
        partial.setWrittenAt(Instant.now());

        out.collect(partial);
    }
}
