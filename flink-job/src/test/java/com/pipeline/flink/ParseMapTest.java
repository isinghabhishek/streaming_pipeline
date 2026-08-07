package com.pipeline.flink;

import com.pipeline.flink.health.MetricsRegistry;
import com.pipeline.flink.model.DLQEvent;
import com.pipeline.flink.model.Event;
import com.pipeline.flink.operator.ParseMap;
import com.pipeline.flink.serde.AvroEventDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.EncoderFactory;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link ParseMap}.
 *
 * <p>Verifies that the operator:
 * <ul>
 *   <li>emits a parsed {@link Event} to the main output on success,</li>
 *   <li>emits a {@link DLQEvent} to the DLQ side output on failure,</li>
 *   <li>increments the {@code parse_errors} counter in {@link MetricsRegistry} on failure,</li>
 *   <li>populates all required DLQ envelope fields (original_topic, error_type, error_message,
 *       raw_bytes).</li>
 * </ul>
 *
 * <p>Uses the Flink {@link OneInputStreamOperatorTestHarness} so tests run without a full
 * Flink cluster while still exercising the real Flink operator lifecycle (open/processElement).
 *
 * <p>Requirements: 3.2
 */
class ParseMapTest {

    private static final String SOURCE_TOPIC = "test-events";

    private static Schema schema;
    private OneInputStreamOperatorTestHarness<byte[], Event> harness;

    @BeforeAll
    static void loadSchema() throws Exception {
        schema = AvroEventDeserializer.loadEventSchema();
    }

    @BeforeEach
    void setUp() throws Exception {
        // Reset the in-process metrics registry before each test
        MetricsRegistry.parseErrors.reset();

        ParseMap operator = new ParseMap(SOURCE_TOPIC);
        harness = new OneInputStreamOperatorTestHarness<>(new ProcessOperator<>(operator));
        harness.open();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (harness != null) {
            harness.close();
        }
    }

    // -------------------------------------------------------------------------
    // Success path tests
    // -------------------------------------------------------------------------

    @Test
    void processElement_validAvroBytes_emitsEventToMainOutput() throws Exception {
        byte[] avroBytes = buildAvroBytes("evt-001", "iot_simulator",
                "2024-01-01T00:00:00Z", Map.of("temp", "22.5"));

        harness.processElement(avroBytes, 1000L);

        List<Event> mainOutput = extractMainOutput();
        assertThat(mainOutput).hasSize(1);

        Event event = mainOutput.get(0);
        assertThat(event.getEventId()).isEqualTo("evt-001");
        assertThat(event.getSource()).isEqualTo("iot_simulator");
        assertThat(event.getTimestamp()).isEqualTo("2024-01-01T00:00:00Z");
        assertThat(event.getPayload()).containsEntry("temp", "22.5");
    }

    @Test
    void processElement_validAvroBytes_doesNotEmitToDLQ() throws Exception {
        byte[] avroBytes = buildAvroBytes("evt-002", "rest_api",
                "2024-06-15T12:00:00Z", Map.of("key", "value"));

        harness.processElement(avroBytes, 1000L);

        List<DLQEvent> dlqOutput = extractDlqOutput();
        assertThat(dlqOutput).isEmpty();
    }

    @Test
    void processElement_validAvroBytes_doesNotIncrementParseErrors() throws Exception {
        byte[] avroBytes = buildAvroBytes("evt-003", "websocket",
                "2024-03-20T08:30:00Z", Collections.emptyMap());

        long errorsBefore = MetricsRegistry.parseErrors.sum();
        harness.processElement(avroBytes, 1000L);

        assertThat(MetricsRegistry.parseErrors.sum()).isEqualTo(errorsBefore);
    }

    @Test
    void processElement_validAvroBytes_emptyPayload_succeeds() throws Exception {
        byte[] avroBytes = buildAvroBytes("evt-004", "rest_api",
                "2024-01-01T00:00:00Z", Collections.emptyMap());

        harness.processElement(avroBytes, 1000L);

        List<Event> mainOutput = extractMainOutput();
        assertThat(mainOutput).hasSize(1);
        assertThat(mainOutput.get(0).getPayload()).isEmpty();
    }

    @Test
    void processElement_confluentWireFormatBytes_stripsHeaderAndSucceeds() throws Exception {
        byte[] avroBytes = buildAvroBytes("evt-005", "iot_simulator",
                "2024-01-01T00:00:00Z", Map.of("sensor", "S42"));

        // Prepend a fake 5-byte Confluent wire-format header: magic 0x00 + 4-byte schema id
        byte[] wireBytes = new byte[avroBytes.length + 5];
        wireBytes[0] = 0x00;
        wireBytes[1] = 0x00;
        wireBytes[2] = 0x00;
        wireBytes[3] = 0x00;
        wireBytes[4] = 0x01; // schema id = 1
        System.arraycopy(avroBytes, 0, wireBytes, 5, avroBytes.length);

        harness.processElement(wireBytes, 1000L);

        List<Event> mainOutput = extractMainOutput();
        assertThat(mainOutput).hasSize(1);
        assertThat(mainOutput.get(0).getEventId()).isEqualTo("evt-005");
    }

    @Test
    void processElement_multipleValidEvents_allEmittedToMainOutput() throws Exception {
        String[] ids = {"id-a", "id-b", "id-c"};
        for (String id : ids) {
            harness.processElement(
                    buildAvroBytes(id, "rest_api", "2024-01-01T00:00:00Z", Map.of()),
                    1000L);
        }

        List<Event> mainOutput = extractMainOutput();
        assertThat(mainOutput).hasSize(3);
        List<String> outputIds = mainOutput.stream().map(Event::getEventId)
                .collect(Collectors.toList());
        assertThat(outputIds).containsExactlyInAnyOrder(ids);
    }

    // -------------------------------------------------------------------------
    // Failure path tests — garbage bytes
    // -------------------------------------------------------------------------

    @Test
    void processElement_garbageBytes_emitsDLQEvent() throws Exception {
        byte[] garbage = new byte[]{1, 2, 3, 4, 5, 6, 7, 8};

        harness.processElement(garbage, 1000L);

        List<DLQEvent> dlqOutput = extractDlqOutput();
        assertThat(dlqOutput).hasSize(1);
    }

    @Test
    void processElement_garbageBytes_mainOutputIsEmpty() throws Exception {
        byte[] garbage = new byte[]{0x42, 0x13, 0x37};

        harness.processElement(garbage, 1000L);

        List<Event> mainOutput = extractMainOutput();
        assertThat(mainOutput).isEmpty();
    }

    @Test
    void processElement_garbageBytes_incrementsParseErrorsCounter() throws Exception {
        byte[] garbage = new byte[]{9, 8, 7, 6, 5};

        long errorsBefore = MetricsRegistry.parseErrors.sum();
        harness.processElement(garbage, 1000L);

        assertThat(MetricsRegistry.parseErrors.sum()).isEqualTo(errorsBefore + 1);
    }

    @Test
    void processElement_garbageBytes_dlqEventHasCorrectOriginalTopic() throws Exception {
        byte[] garbage = new byte[]{1, 2, 3};

        harness.processElement(garbage, 1000L);

        DLQEvent dlqEvent = extractDlqOutput().get(0);
        assertThat(dlqEvent.getOriginalTopic()).isEqualTo(SOURCE_TOPIC);
    }

    @Test
    void processElement_garbageBytes_dlqEventHasErrorTypeAndMessage() throws Exception {
        byte[] garbage = new byte[]{1, 2, 3};

        harness.processElement(garbage, 1000L);

        DLQEvent dlqEvent = extractDlqOutput().get(0);
        assertThat(dlqEvent.getErrorType()).isNotBlank();
        assertThat(dlqEvent.getErrorMessage()).isNotBlank();
    }

    @Test
    void processElement_garbageBytes_dlqEventContainsOriginalRawBytes() throws Exception {
        byte[] garbage = new byte[]{10, 20, 30, 40};

        harness.processElement(garbage, 1000L);

        DLQEvent dlqEvent = extractDlqOutput().get(0);
        assertThat(dlqEvent.getRawBytes()).isEqualTo(garbage);
    }

    @Test
    void processElement_garbageBytes_dlqEventHasSentinelPartitionAndOffset() throws Exception {
        byte[] garbage = new byte[]{1, 2, 3};

        harness.processElement(garbage, 1000L);

        DLQEvent dlqEvent = extractDlqOutput().get(0);
        // partition and offset are unavailable in non-keyed ProcessFunction context
        assertThat(dlqEvent.getOriginalPartition()).isEqualTo(-1);
        assertThat(dlqEvent.getOriginalOffset()).isEqualTo(-1L);
    }

    @Test
    void processElement_emptyBytes_routesToDLQ() throws Exception {
        harness.processElement(new byte[0], 1000L);

        assertThat(extractDlqOutput()).hasSize(1);
        assertThat(extractMainOutput()).isEmpty();
    }

    // -------------------------------------------------------------------------
    // Multiple failure tests — counter must accumulate
    // -------------------------------------------------------------------------

    @Test
    void processElement_multipleGarbageEvents_counterAccumulates() throws Exception {
        int errorCount = 5;
        long errorsBefore = MetricsRegistry.parseErrors.sum();

        for (int i = 0; i < errorCount; i++) {
            harness.processElement(new byte[]{(byte) i, (byte) (i + 1)}, 1000L + i);
        }

        assertThat(MetricsRegistry.parseErrors.sum()).isEqualTo(errorsBefore + errorCount);
        assertThat(extractDlqOutput()).hasSize(errorCount);
        assertThat(extractMainOutput()).isEmpty();
    }

    @Test
    void processElement_mixedValidAndInvalid_routesEachCorrectly() throws Exception {
        byte[] valid1   = buildAvroBytes("mix-valid-1", "rest_api", "2024-01-01T00:00:00Z", Map.of());
        byte[] garbage  = new byte[]{0x01, 0x02, 0x03};
        byte[] valid2   = buildAvroBytes("mix-valid-2", "rest_api", "2024-01-01T00:01:00Z", Map.of());

        harness.processElement(valid1,  1000L);
        harness.processElement(garbage, 2000L);
        harness.processElement(valid2,  3000L);

        List<Event> mainOutput = extractMainOutput();
        List<DLQEvent> dlqOutput = extractDlqOutput();

        assertThat(mainOutput).hasSize(2);
        assertThat(dlqOutput).hasSize(1);
        assertThat(MetricsRegistry.parseErrors.sum()).isEqualTo(1L);
    }

    // -------------------------------------------------------------------------
    // DLQ tag constant test
    // -------------------------------------------------------------------------

    @Test
    void dlqTag_isNonNull_andHasCorrectId() {
        assertThat(ParseMap.DLQ_TAG).isNotNull();
        assertThat(ParseMap.DLQ_TAG.getId()).isEqualTo("dlq-parse-errors");
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Serializes a set of fields to raw Avro binary bytes (no Confluent header).
     */
    private byte[] buildAvroBytes(String eventId, String source, String timestamp,
                                  Map<String, String> payload) throws Exception {
        GenericRecord record = new GenericData.Record(schema);
        record.put("event_id",  eventId);
        record.put("source",    source);
        record.put("timestamp", timestamp);
        record.put("payload",   payload);

        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        org.apache.avro.io.Encoder encoder = EncoderFactory.get().binaryEncoder(baos, null);
        writer.write(record, encoder);
        encoder.flush();
        return baos.toByteArray();
    }

    /**
     * Extracts all main-output {@link Event}s from the test harness output queue.
     */
    @SuppressWarnings("unchecked")
    private List<Event> extractMainOutput() {
        return harness.getOutput().stream()
                .filter(r -> r instanceof StreamRecord)
                .map(r -> (Event) ((StreamRecord<?>) r).getValue())
                .collect(Collectors.toList());
    }

    /**
     * Extracts all side-output {@link DLQEvent}s from the test harness.
     *
     * <p>{@link OneInputStreamOperatorTestHarness#getSideOutput} returns {@code null} when
     * no events have been emitted to that side output tag yet, so we guard against null here.
     */
    @SuppressWarnings("unchecked")
    private List<DLQEvent> extractDlqOutput() {
        java.util.Queue<?> sideOutput = harness.getSideOutput(ParseMap.DLQ_TAG);
        if (sideOutput == null) {
            return Collections.emptyList();
        }
        return sideOutput.stream()
                .filter(r -> r instanceof StreamRecord)
                .map(r -> ((StreamRecord<DLQEvent>) r).getValue())
                .collect(Collectors.toList());
    }
}
