package com.pipeline.flink.serde;

import com.pipeline.flink.model.Event;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Deserializes Kafka messages encoded with Confluent wire format (magic byte + schema-id + Avro).
 *
 * Wire format:
 *   [0x00][4-byte schema-id][avro-binary-payload]
 *
 * This implementation uses a schema baked into the classpath (avro/Event.avsc).
 * In a production setup you would use the Schema Registry client to resolve the
 * schema-id dynamically; for this portfolio project we embed the schema to keep
 * the job self-contained in tests.
 */
public class AvroEventDeserializer implements DeserializationSchema<byte[]> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(AvroEventDeserializer.class);

    /**
     * We return raw bytes here so the ParseMap operator can handle
     * deserialization with full metadata (partition, offset) in context.
     * This class satisfies the DeserializationSchema contract required
     * by the KafkaSource builder.
     */
    @Override
    public byte[] deserialize(byte[] message) throws IOException {
        // pass through — ParseMap does the actual Avro parsing
        return message;
    }

    @Override
    public boolean isEndOfStream(byte[] nextElement) {
        return false;
    }

    @Override
    public TypeInformation<byte[]> getProducedType() {
        return TypeInformation.of(byte[].class);
    }

    // -------------------------------------------------------------------------
    // Static helper used by ParseMap
    // -------------------------------------------------------------------------

    /**
     * Parses Confluent-wire-format Avro bytes into an {@link Event}.
     *
     * @param wireBytes  raw bytes from Kafka (may include 5-byte Confluent header)
     * @return parsed Event
     * @throws IOException if the bytes are not valid Avro
     */
    public static Event parse(byte[] wireBytes) throws IOException {
        byte[] avroBytes = stripConfluentHeader(wireBytes);

        org.apache.avro.Schema schema = loadEventSchema();
        DatumReader<GenericRecord> reader =
                new GenericDatumReader<>(schema);
        org.apache.avro.io.Decoder decoder =
                DecoderFactory.get().binaryDecoder(avroBytes, null);
        GenericRecord record = reader.read(null, decoder);

        String eventId   = record.get("event_id").toString();
        String source    = record.get("source").toString();
        String timestamp = record.get("timestamp").toString();

        // Avro map type deserializes as java.util.Map<org.apache.avro.util.Utf8, Object>
        // (key is Utf8 which implements CharSequence, value is Utf8 for string values)
        @SuppressWarnings("unchecked")
        java.util.Map<Object, Object> rawPayload =
                (java.util.Map<Object, Object>) record.get("payload");

        Map<String, String> payload = new HashMap<>();
        if (rawPayload != null) {
            rawPayload.forEach((k, v) -> payload.put(k.toString(), v != null ? v.toString() : ""));
        }

        return new Event(eventId, source, timestamp, payload);
    }

    /**
     * Strips the 5-byte Confluent Schema Registry wire-format header if present.
     * Header: [0x00][4-byte big-endian schema-id]
     */
    static byte[] stripConfluentHeader(byte[] bytes) {
        if (bytes.length > 5 && bytes[0] == 0x00) {
            byte[] payload = new byte[bytes.length - 5];
            System.arraycopy(bytes, 5, payload, 0, payload.length);
            return payload;
        }
        return bytes;
    }

    /**
     * Loads the Event Avro schema from the classpath resource avro/event.avsc.
     */
    public static org.apache.avro.Schema loadEventSchema() throws IOException {
        try (java.io.InputStream is =
                     AvroEventDeserializer.class.getResourceAsStream("/avro/event.avsc")) {
            if (is == null) {
                throw new IOException("Schema resource /avro/event.avsc not found on classpath");
            }
            return new org.apache.avro.Schema.Parser().parse(is);
        }
    }
}
