package com.pipeline.flink;

// Feature: real-time-streaming-pipeline, Property 7: Checkpoint recovery preserves exactly-once semantics

import com.pipeline.flink.model.Event;
import com.pipeline.flink.operator.EventDeduplicator;
import net.jqwik.api.*;
import net.jqwik.api.constraints.IntRange;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.assertj.core.api.Assertions;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Property 7: Checkpoint recovery preserves exactly-once semantics.
 *
 * <p>For any sequence of events where a failure occurs mid-stream, restarting
 * the Stream Processor from the last successful checkpoint shall produce a final
 * state identical to uninterrupted processing — no events duplicated, no events dropped.
 *
 * <p>Validates: Requirements 3.7, 4.1, 4.2
 *
 * <p>Simulation strategy:
 * <ol>
 *   <li>Process the first batch of events and take a checkpoint snapshot.</li>
 *   <li>Restore from the snapshot (simulating a crash).</li>
 *   <li>Re-process remaining events on the restored harness.</li>
 *   <li>Assert the total output equals uninterrupted processing of the full stream.</li>
 * </ol>
 */
class CheckpointRecoveryTest {

    private static final long WINDOW_SECONDS = 60L;

    // -------------------------------------------------------------------------
    // Property 7a — recovery produces no additional duplicate event_ids
    // -------------------------------------------------------------------------

    @Property(tries = 100)
    @Label("P7a: crash-and-recover produces same event_id set as uninterrupted run")
    void recoveryProducesSameEventIdSet(
            @ForAll("eventStreams") List<Event> stream,
            @ForAll @IntRange(min = 0, max = 5) int splitOffset) throws Exception {

        int splitAt = Math.min(splitOffset, stream.size());

        List<Event> referenceOutput = runUninterrupted(stream);
        List<Event> recoveryOutput  = runWithCrashAndRestore(stream, splitAt);

        Set<String> referenceIds = referenceOutput.stream().map(Event::getEventId).collect(Collectors.toSet());
        Set<String> recoveryIds  = recoveryOutput.stream().map(Event::getEventId).collect(Collectors.toSet());

        Assertions.assertThat(recoveryIds)
                .as("Recovery output event_ids must match reference output")
                .isEqualTo(referenceIds);
    }

    // -------------------------------------------------------------------------
    // Property 7b — recovery drops no events
    // -------------------------------------------------------------------------

    @Property(tries = 100)
    @Label("P7b: crash-and-recover drops no events relative to uninterrupted run")
    void recoveryDropsNoEvents(
            @ForAll("eventStreams") List<Event> stream,
            @ForAll @IntRange(min = 0, max = 5) int splitOffset) throws Exception {

        int splitAt = Math.min(splitOffset, stream.size());
        List<Event> referenceOutput = runUninterrupted(stream);
        List<Event> recoveryOutput  = runWithCrashAndRestore(stream, splitAt);

        Set<String> recoveryIds = recoveryOutput.stream().map(Event::getEventId).collect(Collectors.toSet());

        for (Event ref : referenceOutput) {
            Assertions.assertThat(recoveryIds)
                    .as("Every event in reference output should appear after recovery")
                    .contains(ref.getEventId());
        }
    }

    // -------------------------------------------------------------------------
    // Property 7c — no duplicate event_ids in recovery output
    // -------------------------------------------------------------------------

    @Property(tries = 100)
    @Label("P7c: recovery output has no duplicate event_ids")
    void recoveryOutputHasNoDuplicates(
            @ForAll("eventStreams") List<Event> stream,
            @ForAll @IntRange(min = 0, max = 5) int splitOffset) throws Exception {

        int splitAt = Math.min(splitOffset, stream.size());
        List<Event> recoveryOutput = runWithCrashAndRestore(stream, splitAt);

        long distinctCount = recoveryOutput.stream().map(Event::getEventId).distinct().count();

        Assertions.assertThat((long) recoveryOutput.size())
                .as("No duplicate event_ids in recovery output")
                .isEqualTo(distinctCount);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private List<Event> runUninterrupted(List<Event> stream) throws Exception {
        KeyedOneInputStreamOperatorTestHarness<String, Event, Event> harness = openHarness();
        long ts = 1_700_000_000_000L;
        for (Event e : stream) harness.processElement(e, ts++);
        List<Event> out = collectOutput(harness);
        harness.close();
        return out;
    }

    private List<Event> runWithCrashAndRestore(List<Event> stream, int splitAt) throws Exception {
        List<Event> allOutput = new ArrayList<>();
        long ts = 1_700_000_000_000L;

        // Phase 1: process first batch and take snapshot
        KeyedOneInputStreamOperatorTestHarness<String, Event, Event> harness = openHarness();
        for (int i = 0; i < splitAt && i < stream.size(); i++) {
            harness.processElement(stream.get(i), ts + i);
        }
        allOutput.addAll(collectOutput(harness));
        OperatorSubtaskState snapshot = harness.snapshot(1L, ts + splitAt);
        harness.close();

        // Phase 2: restore and process remainder
        KeyedOneInputStreamOperatorTestHarness<String, Event, Event> restored = openHarness();
        restored.initializeState(snapshot);
        restored.open();
        for (int i = splitAt; i < stream.size(); i++) {
            restored.processElement(stream.get(i), ts + i);
        }
        allOutput.addAll(collectOutput(restored));
        restored.close();

        return allOutput;
    }

    private KeyedOneInputStreamOperatorTestHarness<String, Event, Event> openHarness()
            throws Exception {
        EventDeduplicator dedup = new EventDeduplicator(WINDOW_SECONDS);
        KeyedProcessOperator<String, Event, Event> op = new KeyedProcessOperator<>(dedup);
        KeyedOneInputStreamOperatorTestHarness<String, Event, Event> harness =
                new KeyedOneInputStreamOperatorTestHarness<>(op, Event::getEventId, Types.STRING);
        harness.open();
        return harness;
    }

    private List<Event> collectOutput(
            KeyedOneInputStreamOperatorTestHarness<String, Event, Event> harness) {
        List<Event> out = new ArrayList<>();
        for (Object record : harness.getOutput()) {
            if (record instanceof StreamRecord) {
                @SuppressWarnings("unchecked")
                StreamRecord<Event> sr = (StreamRecord<Event>) record;
                out.add(sr.getValue());
            }
        }
        harness.getOutput().clear();
        return out;
    }

    // -------------------------------------------------------------------------
    // Arbitraries
    // -------------------------------------------------------------------------

    /** Stream of 5–15 events with ~30% duplicates. */
    @Provide
    Arbitrary<List<Event>> eventStreams() {
        return Arbitraries.integers().between(5, 15).flatMap(size -> {
            int uniqueCount = Math.max(1, (int) (size * 0.7));
            int dupCount    = size - uniqueCount;
            return Arbitraries.strings().alpha().ofLength(8).list().ofSize(uniqueCount)
                    .map(ids -> {
                        List<Event> stream = new ArrayList<>();
                        for (String id : ids) stream.add(makeEvent(id));
                        Random rng = new Random(ids.hashCode());
                        for (int i = 0; i < dupCount; i++) {
                            String dupId = ids.get(rng.nextInt(ids.size()));
                            stream.add(makeEvent(dupId));
                        }
                        Collections.shuffle(stream, new Random(7));
                        return stream;
                    });
        });
    }

    private Event makeEvent(String eventId) {
        return new Event(eventId, "iot_simulator",
                Instant.ofEpochSecond(1_700_000_000L).toString(),
                Collections.singletonMap("value", "1.0"));
    }
}
