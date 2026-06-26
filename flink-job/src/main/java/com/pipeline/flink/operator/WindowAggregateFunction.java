package com.pipeline.flink.operator;

import com.pipeline.flink.model.AggregationResult;
import com.pipeline.flink.model.Event;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * Incremental {@link AggregateFunction} that computes count, sum, and average
 * over a tumbling event-time window keyed by {@code source}.
 *
 * <p>The accumulator is a simple POJO carrying running totals. Flink invokes
 * {@link #add} for each event in the window, then {@link #getResult} once when
 * the watermark advances past the window boundary.
 *
 * <p>For the {@code metricSum} / {@code metricAvg} calculations, the function
 * looks for a numeric key called {@code "value"} in the event payload.
 * If that key is absent or non-numeric the event still contributes to the count
 * but not to the sum/avg, which default to 0.0.
 *
 * <p>This satisfies Requirements 3.3 and 3.4 (tumbling-window aggregation,
 * emit result when watermark advances past window boundary).
 */
public class WindowAggregateFunction
        implements AggregateFunction<Event, WindowAggregateFunction.Accumulator, AggregationResult> {

    private static final long serialVersionUID = 1L;

    // -------------------------------------------------------------------------
    // Accumulator POJO
    // -------------------------------------------------------------------------

    /**
     * Mutable accumulator holding running totals for a single window bucket.
     * Must be serializable so Flink can checkpoint it.
     */
    public static class Accumulator implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

        long   count  = 0L;
        double sum    = 0.0;
        String source = "";

        @Override
        public String toString() {
            return "Accumulator{source='" + source + "', count=" + count + ", sum=" + sum + "}";
        }
    }

    // -------------------------------------------------------------------------
    // AggregateFunction contract
    // -------------------------------------------------------------------------

    @Override
    public Accumulator createAccumulator() {
        return new Accumulator();
    }

    @Override
    public Accumulator add(Event event, Accumulator acc) {
        acc.count++;
        acc.source = event.getSource();

        // Attempt to extract a numeric "value" field from the payload
        String rawValue = event.getPayload().get("value");
        if (rawValue != null) {
            try {
                acc.sum += Double.parseDouble(rawValue);
            } catch (NumberFormatException ignored) {
                // non-numeric value — count only, no contribution to sum/avg
            }
        }
        return acc;
    }

    @Override
    public AggregationResult getResult(Accumulator acc) {
        double avg = acc.count > 0 ? acc.sum / acc.count : 0.0;
        // window boundaries are filled in by the companion WindowResultFunction
        return new AggregationResult(
                /* windowStart */ null,
                /* windowEnd   */ null,
                acc.source,
                acc.count,
                acc.sum,
                avg,
                java.time.Instant.now()
        );
    }

    @Override
    public Accumulator merge(Accumulator a, Accumulator b) {
        Accumulator merged = new Accumulator();
        merged.count  = a.count + b.count;
        merged.sum    = a.sum   + b.sum;
        merged.source = a.source.isEmpty() ? b.source : a.source;
        return merged;
    }
}
