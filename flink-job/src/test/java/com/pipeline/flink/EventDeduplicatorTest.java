package com.pipeline.flink;

// Feature: real-time-streaming-pipeline, Property 8: Exactly-once write count

import com.pipeline.flink.model.Event;
import com.pipeline.flink.operator.EventDeduplicator;
import net.jqwik.api.*;
import net.jqwik.api.constraints.IntRange;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.assertj.core.api.Assertions;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Property 8: Exactly-once write count.
 *
 * <p>For any stream of events (including duplicates) published to Kafka within
 * a time window, the number of records written to the Table Store for that window
 * shall equal the number of unique {@code event_id} values in the stream.
 *
 * <p>Validates: Requirements 4.3, 4.4, 4.5
 *
 * <p>Uses the Flink {@link KeyedOneInputStreamOperatorTestHarness} to run
 * {@link EventDeduplicator} in a real Flink mini-runtime without a full cluster.
 */
class EventDeduplicatorTest {

    private static final long WINDOW_SIZE_SECONDS = 60L;

    // -------------------------------------------------------------------------
    // Property 8a — unique event_ids pass through exactly once
    // -------------------------------------------------------------------------

    @Property(tries = 150)
    @Label("P8a: unique events all pass through deduplicator")
    void uniqueEventsAllPassThrough(@ForAll("uniqueEventLists") List<Event> events)
            throws Exception {

        List<Event> output = runDeduplicator(events);

        Assertions.assertThat(output)
                .as("All unique events should pass through")
                .hasSize(events.size());
    }

    // -------------------------------------------------------------------------
    // Property 8b — duplicates are suppressed; output count = unique event_id count
    // -------------------------------------------------------------------------

    @Property(tries = 150)
    @Label("P8b: output count equals number of unique event_ids")
    void outputCountEqualsUniqueEventIds(@ForAll("streamsWithDuplicates") List<Event> stream)
            throws Exception {

        long uniqueCount = stream.stream().map(Event::getEventId).distinct().count();
        List<Event> output = runDeduplicator(stream);

        Assertions.assertThat(output)
                .as("Output size should equal unique event_id count")
                .hasSize((int) uniqueCount);
    }

    // -------------------------------------------------------------------------
    // Property 8c — every output event_id was present in the input
    // -------------------------------------------------------------------------

    @Property(tries = 100)
    @Label("P8c: every output event_id appeared in the input")
    void outputEventIdsAreSubsetOfInput(@ForAll("streamsWithDuplicates") List<Event> stream)
            throws Exception {

        Set<String> inputIds = stream.stream().map(Event::getEventId).collect(Collectors.toSet());
        List<Event> output = runDeduplicator(stream);

        for (Event e : output) {
            Assertions.assertThat(inputIds)
                    .as("Output event_id must exist in input")
                    .contains(e.getEventId());
        }
    }

    // -------------------------------------------------------------------------
    // Property 8d — output event_ids are all distinct (no duplicates leaked)
    // -------------------------------------------------------------------------

    @Property(tries = 150)
    @Label("P8d: no duplicate event_ids in the output")
    void noOutputDuplicates(@ForAll("streamsWithDuplicates") List<Event> stream) throws Exception {

        List<Event> output = runDeduplicator(stream);
        long distinctOutput = output.stream().map(Event::getEventId).distinct().count();

        Assertions.assertThat(distinctOutput)
                .as("No duplicate event_ids should appear in output")
                .isEqualTo(output.size());
    }

    // -------------------------------------------------------------------------
    // Property 8e — zero-duplicate stream: all events pass through
    // -------------------------------------------------------------------------

    @Property(tries = 100)
    @Label("P8e: stream with zero duplicates — all events pass through")
    void zeroDuplicateRateAllPass(@ForAll("uniqueEventLists") List<Event> events) throws Exception {
        List<Event> output = runDeduplicator(events);
        Assertions.assertThat(output).hasSize(events.size());
    }

    // -------------------------------------------------------------------------
    // Property 8f — 100% duplicate stream: only 1 event passes per unique id
    // -------------------------------------------------------------------------

    @Property(tries = 100)
    @Label("P8f: all-duplicate stream passes exactly one event per unique id")
    void allDuplicatesOnlyOnePassesPerKey(
            @ForAll("smallUniqueEventLists") List<Event> distinctEvents,
            @ForAll @IntRange(min = 2, max = 5) int duplicateFactor) throws Exception {

        List<Event> stream = new ArrayList<>();
        for (Event e : distinctEvents) {
            for (int i = 0; i < duplicateFactor; i++) stream.add(e);
        }

        List<Event> output = runDeduplicator(stream);

        Assertions.assertThat(output)
                .as("Only one copy of each event should pass through")
                .hasSize(distinctEvents.size());
    }

    // -------------------------------------------------------------------------
    // Test harness helper
    // -------------------------------------------------------------------------

    private List<Event> runDeduplicator(List<Event> events) throws Exception {
        EventDeduplicator dedup = new EventDeduplicator(WINDOW_SIZE_SECONDS);
        KeyedProcessOperator<String, Event, Event> operator = new KeyedProcessOperator<>(dedup);
        KeyedOneInputStreamOperatorTestHarness<String, Event, Event> harness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        operator, Event::getEventId, Types.STRING);
        harness.open();

        long timestamp = 1_700_000_000_000L;
        for (Event e : events) {
            harness.processElement(e, timestamp++);
        }

        List<Event> output = new ArrayList<>();
        for (Object record : harness.getOutput()) {
            if (record instanceof StreamRecord) {
                @SuppressWarnings("unchecked")
                StreamRecord<Event> sr = (StreamRecord<Event>) record;
                output.add(sr.getValue());
            }
        }

        harness.close();
        return output;
    }

    // -------------------------------------------------------------------------
    // Arbitraries
    // -------------------------------------------------------------------------

    @Provide
    Arbitrary<Event> uniqueEvent() {
        return Combinators.combine(
                Arbitraries.strings().alpha().ofMinLength(8).ofMaxLength(36)
                        .map(s -> UUID.randomUUID().toString()),
                Arbitraries.of("rest_api", "iot_simulator", "websocket"),
                Arbitraries.just(Instant.ofEpochSecond(1_700_000_000L).toString()),
                Arbitraries.just(Collections.singletonMap("value", "42.0"))
        ).as(Event::new);
    }

    @Provide
    Arbitrary<List<Event>> uniqueEventLists() {
        return uniqueEvent().list().ofMinSize(1).ofMaxSize(50);
    }

    @Provide
    Arbitrary<List<Event>> smallUniqueEventLists() {
        return uniqueEvent().list().ofMinSize(1).ofMaxSize(20);
    }

    /**
     * Streams where 0–50% of entries are duplicates of earlier event_ids.
     */
    @Provide
    Arbitrary<List<Event>> streamsWithDuplicates() {
        return Arbitraries.integers().between(5, 40).flatMap(size -> {
            int uniqueCount = Math.max(1, size / 2);
            int dupCount    = size - uniqueCount;
            return uniqueEvent().list().ofSize(uniqueCount).map(uniqueList -> {
                List<Event> stream = new ArrayList<>(uniqueList);
                if (!uniqueList.isEmpty()) {
                    Random rng = new Random(uniqueList.hashCode());
                    for (int i = 0; i < dupCount; i++) {
                        Event original = uniqueList.get(rng.nextInt(uniqueList.size()));
                        stream.add(new Event(
                                original.getEventId(), original.getSource(),
                                original.getTimestamp(), original.getPayload()));
                    }
                }
                Collections.shuffle(stream, new Random(42));
                return stream;
            });
        });
    }
}
