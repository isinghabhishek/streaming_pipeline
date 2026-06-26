package com.pipeline.flink.source;

import com.pipeline.flink.serde.AvroEventDeserializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.time.Duration;

/**
 * Factory for the Flink {@link KafkaSource} and its accompanying {@link WatermarkStrategy}.
 *
 * <p>The source:
 * <ul>
 *   <li>uses a named consumer group so Kafka brokers track committed offsets per partition</li>
 *   <li>starts from committed offsets (earliest on first run)</li>
 *   <li>delivers raw bytes to the downstream {@code ParseMap} operator</li>
 * </ul>
 *
 * <p>The watermark strategy:
 * <ul>
 *   <li>is <em>event-time</em> based, extracted from the {@code timestamp} field after parsing</li>
 *   <li>uses a bounded-out-of-orderness of 5 seconds to tolerate minor event reordering</li>
 *   <li>idles after 10 seconds of no messages so windowed operators are not stalled</li>
 * </ul>
 */
public class KafkaSourceConfig {

    private KafkaSourceConfig() {}

    /**
     * Builds a {@link KafkaSource} that reads raw (byte[]) messages from the given topic.
     *
     * @param bootstrapServers  Kafka bootstrap server list (e.g. "kafka:9092")
     * @param topic             source topic name
     * @param consumerGroup     consumer group id
     */
    public static KafkaSource<byte[]> buildKafkaSource(String bootstrapServers,
                                                       String topic,
                                                       String consumerGroup) {
        return KafkaSource.<byte[]>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(consumerGroup)
                // Start from committed offsets; fall back to earliest on first run
                .setStartingOffsets(OffsetsInitializer.committedOffsets(
                        org.apache.kafka.clients.consumer.OffsetResetStrategy.EARLIEST))
                .setValueOnlyDeserializer(new AvroEventDeserializer())
                .build();
    }

    /**
     * Returns a {@link WatermarkStrategy} that:
     * <ul>
     *   <li>uses bounded out-of-orderness (5 s) to handle minor reordering</li>
     *   <li>idles after 10 s of silence so downstream windows can still close</li>
     * </ul>
     *
     * <p>The actual event-time extraction from the {@code timestamp} string happens inside
     * {@link com.pipeline.flink.operator.ParseMap} via
     * {@link org.apache.flink.streaming.api.datastream.DataStream#assignTimestampsAndWatermarks}
     * after deserialization, where the {@link com.pipeline.flink.model.Event} object is available.
     */
    public static WatermarkStrategy<byte[]> buildRawWatermarkStrategy() {
        // Raw bytes carry no event-time; we assign ingestion time as a placeholder.
        // Event-time watermarks are re-assigned on the parsed stream in StreamingJob.
        return WatermarkStrategy
                .<byte[]>forMonotonousTimestamps()
                .withIdleness(Duration.ofSeconds(10));
    }

    /**
     * Returns an event-time {@link WatermarkStrategy} for the parsed
     * {@link com.pipeline.flink.model.Event} stream.
     *
     * <p>The timestamp extractor parses the ISO-8601 UTC string from the event
     * and returns epoch milliseconds.
     */
    public static WatermarkStrategy<com.pipeline.flink.model.Event> buildEventWatermarkStrategy() {
        return WatermarkStrategy
                .<com.pipeline.flink.model.Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((event, recordTimestamp) -> {
                    try {
                        return java.time.Instant.parse(event.getTimestamp()).toEpochMilli();
                    } catch (Exception e) {
                        // Fall back to Kafka ingestion time if the timestamp cannot be parsed
                        return recordTimestamp;
                    }
                })
                .withIdleness(Duration.ofSeconds(10));
    }
}
