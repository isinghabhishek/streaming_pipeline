package com.pipeline.flink.model;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

/**
 * Avro-deserialized event record consumed from Kafka.
 * Mirrors the Event Avro schema in schemas/event.avsc.
 */
public class Event implements Serializable {

    private static final long serialVersionUID = 1L;

    private String eventId;
    private String source;
    private String timestamp;   // ISO-8601 UTC string
    private Map<String, String> payload;

    public Event() {}

    public Event(String eventId, String source, String timestamp, Map<String, String> payload) {
        this.eventId = Objects.requireNonNull(eventId, "eventId must not be null");
        this.source = Objects.requireNonNull(source, "source must not be null");
        this.timestamp = Objects.requireNonNull(timestamp, "timestamp must not be null");
        this.payload = Objects.requireNonNull(payload, "payload must not be null");
    }

    // -------------------------------------------------------------------------
    // Getters / setters
    // -------------------------------------------------------------------------

    public String getEventId()    { return eventId; }
    public String getSource()     { return source; }
    public String getTimestamp()  { return timestamp; }
    public Map<String, String> getPayload() { return payload; }

    public void setEventId(String eventId)     { this.eventId = eventId; }
    public void setSource(String source)       { this.source = source; }
    public void setTimestamp(String timestamp) { this.timestamp = timestamp; }
    public void setPayload(Map<String, String> payload) { this.payload = payload; }

    // -------------------------------------------------------------------------
    // Equality (used in tests)
    // -------------------------------------------------------------------------

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Event)) return false;
        Event other = (Event) o;
        return Objects.equals(eventId, other.eventId)
                && Objects.equals(source, other.source)
                && Objects.equals(timestamp, other.timestamp)
                && Objects.equals(payload, other.payload);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventId, source, timestamp, payload);
    }

    @Override
    public String toString() {
        return "Event{eventId='" + eventId + "', source='" + source
                + "', timestamp='" + timestamp + "', payload=" + payload + "}";
    }
}
