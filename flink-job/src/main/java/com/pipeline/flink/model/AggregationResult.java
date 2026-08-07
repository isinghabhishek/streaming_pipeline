package com.pipeline.flink.model;

import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

/**
 * Output record for a completed tumbling-window aggregation.
 * Written to the Delta Lake "aggregations" table.
 */
public class AggregationResult implements Serializable {

    private static final long serialVersionUID = 1L;

    private Instant windowStart;
    private Instant windowEnd;
    private String  source;
    private long    eventCount;
    private double  metricSum;
    private double  metricAvg;
    private Instant writtenAt;

    public AggregationResult() {}

    public AggregationResult(Instant windowStart, Instant windowEnd, String source,
                             long eventCount, double metricSum, double metricAvg,
                             Instant writtenAt) {
        this.windowStart = windowStart;   // may be null until WindowResultFunction fills it in
        this.windowEnd   = windowEnd;     // may be null until WindowResultFunction fills it in
        this.source      = Objects.requireNonNull(source);
        this.eventCount  = eventCount;
        this.metricSum   = metricSum;
        this.metricAvg   = metricAvg;
        this.writtenAt   = Objects.requireNonNull(writtenAt);
    }

    // -------------------------------------------------------------------------
    // Getters / setters
    // -------------------------------------------------------------------------

    public Instant getWindowStart() { return windowStart; }
    public Instant getWindowEnd()   { return windowEnd; }
    public String  getSource()      { return source; }
    public long    getEventCount()  { return eventCount; }
    public double  getMetricSum()   { return metricSum; }
    public double  getMetricAvg()   { return metricAvg; }
    public Instant getWrittenAt()   { return writtenAt; }

    public void setWindowStart(Instant v) { this.windowStart = v; }
    public void setWindowEnd(Instant v)   { this.windowEnd = v; }
    public void setSource(String v)       { this.source = v; }
    public void setEventCount(long v)     { this.eventCount = v; }
    public void setMetricSum(double v)    { this.metricSum = v; }
    public void setMetricAvg(double v)    { this.metricAvg = v; }
    public void setWrittenAt(Instant v)   { this.writtenAt = v; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AggregationResult)) return false;
        AggregationResult other = (AggregationResult) o;
        return eventCount == other.eventCount
                && Double.compare(metricSum, other.metricSum) == 0
                && Double.compare(metricAvg, other.metricAvg) == 0
                && Objects.equals(windowStart, other.windowStart)
                && Objects.equals(windowEnd, other.windowEnd)
                && Objects.equals(source, other.source);
    }

    @Override
    public int hashCode() {
        return Objects.hash(windowStart, windowEnd, source, eventCount, metricSum, metricAvg);
    }

    @Override
    public String toString() {
        return "AggregationResult{source='" + source + "', window=[" + windowStart + ", "
                + windowEnd + "), count=" + eventCount + ", sum=" + metricSum
                + ", avg=" + metricAvg + "}";
    }
}
