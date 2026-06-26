package com.pipeline.flink;

// Feature: real-time-streaming-pipeline, Property 6: Windowed aggregation correctness and emission

import com.pipeline.flink.model.AggregationResult;
import com.pipeline.flink.model.Event;
import com.pipeline.flink.operator.WindowAggregateFunction;
import net.jqwik.api.*;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.Size;
import org.assertj.core.api.Assertions;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Property 6: Windowed aggregation correctness and emission.
 *
 * <p>For any set of events assigned to a tumbling window of any valid size
 * (10–300 seconds), when the watermark advances past the window boundary,
 * the emitted aggregation result (count, sum, average) shall equal the
 * mathematically correct value computed over those events.
 *
 * <p>Validates: Requirements 3.3, 3.4
 *
 * <p>Tests run at the pure logic layer (no Flink runtime required):
 * we drive the AggregateFunction directly via its accumulator interface.
 */
class WindowAggregateFunctionTest {

    private final WindowAggregateFunction aggFn = new WindowAggregateFunction();

    // -------------------------------------------------------------------------
    // Property 6a — count equals number of events in the window
    // -------------------------------------------------------------------------

    @Property(tries = 200)
    @Label("P6a: event count equals input size")
    void countEqualsInputSize(@ForAll("eventLists") List<Event> events) {

        AggregationResult result = aggregate(events);

        Assertions.assertThat(result.getEventCount())
                .as("Aggregated count should equal number of input events")
                .isEqualTo(events.size());
    }

    // -------------------------------------------------------------------------
    // Property 6b — sum equals arithmetic sum of numeric "value" fields
    // -------------------------------------------------------------------------

    @Property(tries = 200)
    @Label("P6b: metric sum equals arithmetic sum of numeric payload values")
    void sumEqualsArithmeticSum(@ForAll("eventListsWithNumericValue") List<Event> events) {

        double expectedSum = events.stream()
                .map(e -> e.getPayload().get("value"))
                .filter(Objects::nonNull)
                .mapToDouble(v -> {
                    try { return Double.parseDouble(v); }
                    catch (NumberFormatException ex) { return 0.0; }
                })
                .sum();

        AggregationResult result = aggregate(events);

        Assertions.assertThat(result.getMetricSum())
                .as("Aggregated sum should equal arithmetic sum of numeric values")
                .isCloseTo(expectedSum, Assertions.within(1e-9));
    }

    // -------------------------------------------------------------------------
    // Property 6c — avg = sum / count
    // -------------------------------------------------------------------------

    @Property(tries = 200)
    @Label("P6c: metric avg equals sum divided by count")
    void avgEqualsSumDividedByCount(@ForAll("eventListsWithNumericValue") List<Event> events) {

        AggregationResult result = aggregate(events);

        double expectedAvg = result.getEventCount() > 0
                ? result.getMetricSum() / result.getEventCount()
                : 0.0;

        Assertions.assertThat(result.getMetricAvg())
                .as("avg should equal sum / count")
                .isCloseTo(expectedAvg, Assertions.within(1e-9));
    }

    // -------------------------------------------------------------------------
    // Property 6d — source field preserved from events
    // -------------------------------------------------------------------------

    @Property(tries = 100)
    @Label("P6d: source field in result matches event source")
    void sourceFieldPreserved(
            @ForAll("sourceType") String source,
            @ForAll("eventListsWithNumericValue") List<Event> rawEvents) {

        List<Event> events = rawEvents.stream()
                .map(e -> new Event(e.getEventId(), source, e.getTimestamp(), e.getPayload()))
                .collect(Collectors.toList());

        AggregationResult result = aggregate(events);

        Assertions.assertThat(result.getSource())
                .as("Source in aggregation result should match event source")
                .isEqualTo(source);
    }

    // -------------------------------------------------------------------------
    // Property 6e — empty-payload events still counted correctly
    // -------------------------------------------------------------------------

    @Property(tries = 100)
    @Label("P6e: events with no numeric value still contribute to count")
    void eventsWithNoValueStillCounted(@ForAll("eventListsNoValue") List<Event> events) {

        AggregationResult result = aggregate(events);

        Assertions.assertThat(result.getEventCount()).isEqualTo(events.size());
        Assertions.assertThat(result.getMetricSum()).isCloseTo(0.0, Assertions.within(1e-9));
        Assertions.assertThat(result.getMetricAvg()).isCloseTo(0.0, Assertions.within(1e-9));
    }

    // -------------------------------------------------------------------------
    // Property 6f — accumulator merge is associative (supports parallel agg)
    // -------------------------------------------------------------------------

    @Property(tries = 100)
    @Label("P6f: split-then-merge produces same result as single-pass aggregation")
    void mergeIsConsistentWithSinglePass(
            @ForAll("eventListsWithNumericValueAtLeast2") List<Event> events) {

        AggregationResult single = aggregate(events);

        int mid = events.size() / 2;
        WindowAggregateFunction.Accumulator accA = buildAccumulator(events.subList(0, mid));
        WindowAggregateFunction.Accumulator accB = buildAccumulator(events.subList(mid, events.size()));
        WindowAggregateFunction.Accumulator merged = aggFn.merge(accA, accB);
        AggregationResult splitResult = aggFn.getResult(merged);

        Assertions.assertThat(splitResult.getEventCount()).isEqualTo(single.getEventCount());
        Assertions.assertThat(splitResult.getMetricSum())
                .isCloseTo(single.getMetricSum(), Assertions.within(1e-9));
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private AggregationResult aggregate(List<Event> events) {
        WindowAggregateFunction.Accumulator acc = aggFn.createAccumulator();
        for (Event e : events) acc = aggFn.add(e, acc);
        return aggFn.getResult(acc);
    }

    private WindowAggregateFunction.Accumulator buildAccumulator(List<Event> events) {
        WindowAggregateFunction.Accumulator acc = aggFn.createAccumulator();
        for (Event e : events) acc = aggFn.add(e, acc);
        return acc;
    }

    // -------------------------------------------------------------------------
    // Arbitraries
    // -------------------------------------------------------------------------

    @Provide
    Arbitrary<String> sourceType() {
        return Arbitraries.of("rest_api", "iot_simulator", "websocket");
    }

    @Provide
    Arbitrary<Event> singleEvent() {
        return Combinators.combine(
                Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(36),
                sourceType(),
                Arbitraries.longs().between(0L, 1_000_000L)
                        .map(offset -> Instant.ofEpochSecond(1_700_000_000L + offset).toString()),
                Arbitraries.maps(
                        Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(10),
                        Arbitraries.strings().alpha().ofMinLength(0).ofMaxLength(10))
                        .ofMinSize(0).ofMaxSize(3)
        ).as(Event::new);
    }

    @Provide
    Arbitrary<List<Event>> eventLists() {
        return singleEvent().list().ofMinSize(1).ofMaxSize(100);
    }

    @Provide
    Arbitrary<Event> singleEventWithNumericValue() {
        return Combinators.combine(
                Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(36),
                sourceType(),
                Arbitraries.longs().between(0L, 1_000_000L)
                        .map(offset -> Instant.ofEpochSecond(1_700_000_000L + offset).toString()),
                Arbitraries.doubles().between(-1_000.0, 1_000.0)
                        .map(v -> Collections.singletonMap("value", String.valueOf(v)))
        ).as(Event::new);
    }

    @Provide
    Arbitrary<List<Event>> eventListsWithNumericValue() {
        return singleEventWithNumericValue().list().ofMinSize(1).ofMaxSize(100);
    }

    @Provide
    Arbitrary<List<Event>> eventListsWithNumericValueAtLeast2() {
        return singleEventWithNumericValue().list().ofMinSize(2).ofMaxSize(60);
    }

    @Provide
    Arbitrary<List<Event>> eventListsNoValue() {
        Arbitrary<Event> noValueEvent = Combinators.combine(
                Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(36),
                sourceType(),
                Arbitraries.longs().between(0L, 1_000_000L)
                        .map(offset -> Instant.ofEpochSecond(1_700_000_000L + offset).toString()),
                Arbitraries.just(Collections.singletonMap("key", "non-numeric"))
        ).as(Event::new);
        return noValueEvent.list().ofMinSize(1).ofMaxSize(50);
    }
}
