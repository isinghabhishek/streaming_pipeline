package com.pipeline.flink.model;

import java.io.Serializable;
import java.util.Objects;

/**
 * Dead-Letter Queue envelope.
 * Mirrors the DLQEvent Avro schema in schemas/dlq_event.avsc.
 */
public class DLQEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    private String originalTopic;
    private int    originalPartition;
    private long   originalOffset;
    private String errorType;
    private String errorMessage;
    private byte[] rawBytes;

    public DLQEvent() {}

    public DLQEvent(String originalTopic, int originalPartition, long originalOffset,
                    String errorType, String errorMessage, byte[] rawBytes) {
        this.originalTopic     = Objects.requireNonNull(originalTopic);
        this.originalPartition = originalPartition;
        this.originalOffset    = originalOffset;
        this.errorType         = Objects.requireNonNull(errorType);
        this.errorMessage      = Objects.requireNonNull(errorMessage);
        this.rawBytes          = Objects.requireNonNull(rawBytes);
    }

    // -------------------------------------------------------------------------
    // Getters / setters
    // -------------------------------------------------------------------------

    public String getOriginalTopic()     { return originalTopic; }
    public int    getOriginalPartition() { return originalPartition; }
    public long   getOriginalOffset()    { return originalOffset; }
    public String getErrorType()         { return errorType; }
    public String getErrorMessage()      { return errorMessage; }
    public byte[] getRawBytes()          { return rawBytes; }

    public void setOriginalTopic(String v)     { this.originalTopic = v; }
    public void setOriginalPartition(int v)    { this.originalPartition = v; }
    public void setOriginalOffset(long v)      { this.originalOffset = v; }
    public void setErrorType(String v)         { this.errorType = v; }
    public void setErrorMessage(String v)      { this.errorMessage = v; }
    public void setRawBytes(byte[] v)          { this.rawBytes = v; }

    @Override
    public String toString() {
        return "DLQEvent{originalTopic='" + originalTopic + "', partition=" + originalPartition
                + ", offset=" + originalOffset + ", errorType='" + errorType + "'}";
    }
}
