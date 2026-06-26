package com.pipeline.flink;

import com.pipeline.flink.model.Event;
import com.pipeline.flink.serde.AvroEventDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for {@link AvroEventDeserializer}.
 * Validates Avro round-trip, Confluent header stripping, and schema loading.
 */
class AvroEventDeserializerTest {

    private static Schema schema;

    @BeforeAll
    static void loadSchema() throws Exception {
        schema = AvroEventDeserializer.loadEventSchema();
    }

    @Test
    void loadEventSchema_loadsSuccessfully() {
        assertThat(schema).isNotNull();
        assertThat(schema.getName()).isEqualTo("Event");
        assertThat(schema.getField("event_id")).isNotNull();
        assertThat(schema.getField("source")).isNotNull();
        assertThat(schema.getField("timestamp")).isNotNull();
        assertThat(schema.getField("payload")).isNotNull();
    }

    @Test
    void parse_roundTripDeserializesCorrectly() throws Exception {
        GenericRecord record = new GenericData.Record(schema);
        record.put("event_id",  "test-uuid-1234");
        record.put("source",    "iot_simulator");
        record.put("timestamp", "2024-01-01T00:00:00Z");
        record.put("payload",   Map.of("sensor_id", "S1", "value", "23.5"));

        byte[] avroBytes = toAvroBytes(record);

        Event event = AvroEventDeserializer.parse(avroBytes);

        assertThat(event.getEventId()).isEqualTo("test-uuid-1234");
        assertThat(event.getSource()).isEqualTo("iot_simulator");
        assertThat(event.getTimestamp()).isEqualTo("2024-01-01T00:00:00Z");
        assertThat(event.getPayload()).containsEntry("sensor_id", "S1");
        assertThat(event.getPayload()).containsEntry("value", "23.5");
    }

    @Test
    void parse_handlesEmptyPayload() throws Exception {
        GenericRecord record = new GenericData.Record(schema);
        record.put("event_id",  "empty-payload");
        record.put("source",    "rest_api");
        record.put("timestamp", "2024-06-01T12:00:00Z");
        record.put("payload",   Map.of());

        byte[] avroBytes = toAvroBytes(record);
        Event event = AvroEventDeserializer.parse(avroBytes);

        assertThat(event.getPayload()).isEmpty();
    }

    @Test
    void stripConfluentHeader_removesHeaderWhenMagicBytePresent() {
        byte[] payload = new byte[]{10, 20, 30};
        byte[] wire    = new byte[]{0x00, 0, 0, 0, 42, 10, 20, 30}; // magic + 4-byte schema-id + payload

        byte[] stripped = AvroEventDeserializer.stripConfluentHeader(wire);

        assertThat(stripped).isEqualTo(payload);
    }

    @Test
    void stripConfluentHeader_passesThrough_whenNoMagicByte() {
        byte[] bytes = new byte[]{0x01, 0x02, 0x03};
        assertThat(AvroEventDeserializer.stripConfluentHeader(bytes)).isEqualTo(bytes);
    }

    @Test
    void parse_throwsOnGarbage() {
        assertThatThrownBy(() -> AvroEventDeserializer.parse(new byte[]{1, 2, 3, 4, 5}))
                .isInstanceOf(Exception.class);
    }

    // -------------------------------------------------------------------------
    // Helper
    // -------------------------------------------------------------------------

    private byte[] toAvroBytes(GenericRecord record) throws Exception {
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        org.apache.avro.io.Encoder encoder = EncoderFactory.get().binaryEncoder(baos, null);
        writer.write(record, encoder);
        encoder.flush();
        return baos.toByteArray();
    }
}
