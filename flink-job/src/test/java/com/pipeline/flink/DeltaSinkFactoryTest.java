package com.pipeline.flink;

import com.pipeline.flink.model.AggregationResult;
import com.pipeline.flink.model.Event;
import com.pipeline.flink.sink.DeltaSinkFactory;
import io.delta.flink.sink.DeltaSink;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Unit tests for {@link DeltaSinkFactory}.
 *
 * <p>Covers:
 * <ul>
 *   <li>Schema correctness for {@code raw_events} and {@code aggregations} tables</li>
 *   <li>Row conversion: {@link DeltaSinkFactory#toRawEventsRow(Event)}</li>
 *   <li>Row conversion: {@link DeltaSinkFactory#toAggregationsRow(AggregationResult)}</li>
 *   <li>Sink instantiation without errors (smoke test for configuration)</li>
 * </ul>
 *
 * <p>Requirements: 3.5, 4.1, 4.3, 5.1, 5.2
 */
class DeltaSinkFactoryTest {

    // -------------------------------------------------------------------------
    // Schema — raw_events
    // -------------------------------------------------------------------------

    @Test
    void rawEventsRowType_hasCorrectFieldCount() {
        RowType schema = DeltaSinkFactory.rawEventsRowType();
        // event_id, source, timestamp, date, payload, ingested_at
        assertThat(schema.getFieldCount()).isEqualTo(6);
    }

    @Test
    void rawEventsRowType_fieldTypesMatchDesign() {
        RowType schema = DeltaSinkFactory.rawEventsRowType();
        assertThat(schema.getTypeAt(0)).isInstanceOf(VarCharType.class);    // event_id
        assertThat(schema.getTypeAt(1)).isInstanceOf(VarCharType.class);    // source
        assertThat(schema.getTypeAt(2)).isInstanceOf(TimestampType.class);  // timestamp
        assertThat(schema.getTypeAt(3)).isInstanceOf(VarCharType.class);    // date
        assertThat(schema.getTypeAt(4)).isInstanceOf(MapType.class);        // payload
        assertThat(schema.getTypeAt(5)).isInstanceOf(TimestampType.class);  // ingested_at
    }

    @Test
    void rawEventsRowType_fieldNamesMatchDesign() {
        RowType schema = DeltaSinkFactory.rawEventsRowType();
        assertThat(schema.getFieldNames())
                .containsExactly("event_id", "source", "timestamp", "date", "payload", "ingested_at");
    }

    @Test
    void rawEventsRowType_payloadMapHasStringKeyAndValue() {
        RowType schema = DeltaSinkFactory.rawEventsRowType();
        MapType mapType = (MapType) schema.getTypeAt(4);
        assertThat(mapType.getKeyType()).isInstanceOf(VarCharType.class);
        assertThat(mapType.getValueType()).isInstanceOf(VarCharType.class);
    }

    // -------------------------------------------------------------------------
    // Schema — aggregations
    // -------------------------------------------------------------------------

    @Test
    void aggregationsRowType_hasCorrectFieldCount() {
        RowType schema = DeltaSinkFactory.aggregationsRowType();
        // window_start, window_end, source, event_count, metric_sum, metric_avg, written_at
        assertThat(schema.getFieldCount()).isEqualTo(7);
    }

    @Test
    void aggregationsRowType_fieldTypesMatchDesign() {
        RowType schema = DeltaSinkFactory.aggregationsRowType();
        assertThat(schema.getTypeAt(0)).isInstanceOf(TimestampType.class); // window_start
        assertThat(schema.getTypeAt(1)).isInstanceOf(TimestampType.class); // window_end
        assertThat(schema.getTypeAt(2)).isInstanceOf(VarCharType.class);   // source
        assertThat(schema.getTypeAt(3)).isInstanceOf(BigIntType.class);    // event_count
        assertThat(schema.getTypeAt(4)).isInstanceOf(DoubleType.class);    // metric_sum
        assertThat(schema.getTypeAt(5)).isInstanceOf(DoubleType.class);    // metric_avg
        assertThat(schema.getTypeAt(6)).isInstanceOf(TimestampType.class); // written_at
    }

    @Test
    void aggregationsRowType_fieldNamesMatchDesign() {
        RowType schema = DeltaSinkFactory.aggregationsRowType();
        assertThat(schema.getFieldNames())
                .containsExactly("window_start", "window_end", "source",
                        "event_count", "metric_sum", "metric_avg", "written_at");
    }

    // -------------------------------------------------------------------------
    // Row conversion — toRawEventsRow
    // -------------------------------------------------------------------------

    @Test
    void toRawEventsRow_setsEventIdCorrectly() {
        Event event = makeEvent("evt-001", "rest_api", "2024-01-15T12:30:00Z", new HashMap<>());
        RowData row = DeltaSinkFactory.toRawEventsRow(event);
        assertThat(row.getString(0).toString()).isEqualTo("evt-001");
    }

    @Test
    void toRawEventsRow_setsSourceCorrectly() {
        Event event = makeEvent("evt-002", "iot_simulator", "2024-01-15T12:30:00Z", new HashMap<>());
        RowData row = DeltaSinkFactory.toRawEventsRow(event);
        assertThat(row.getString(1).toString()).isEqualTo("iot_simulator");
    }

    @Test
    void toRawEventsRow_parsesTimestampToTimestampData() {
        String ts = "2024-01-15T12:30:00Z";
        Event event = makeEvent("evt-003", "rest_api", ts, new HashMap<>());
        RowData row = DeltaSinkFactory.toRawEventsRow(event);

        TimestampData tsData = row.getTimestamp(2, 3);
        assertThat(tsData).isNotNull();
        long expectedMillis = Instant.parse(ts).toEpochMilli();
        assertThat(tsData.toLocalDateTime().toInstant(java.time.ZoneOffset.UTC).toEpochMilli())
                .isEqualTo(expectedMillis);
    }

    @Test
    void toRawEventsRow_derivesDatePartitionFromTimestamp() {
        Event event = makeEvent("evt-004", "rest_api", "2024-06-20T23:59:59Z", new HashMap<>());
        RowData row = DeltaSinkFactory.toRawEventsRow(event);
        // Date derived from UTC timestamp
        assertThat(row.getString(3).toString()).isEqualTo("2024-06-20");
    }

    @Test
    void toRawEventsRow_setsPayloadMap() {
        Map<String, String> payload = new HashMap<>();
        payload.put("value", "42.5");
        payload.put("sensor", "temp01");
        Event event = makeEvent("evt-005", "iot_simulator", "2024-01-15T00:00:00Z", payload);
        RowData row = DeltaSinkFactory.toRawEventsRow(event);

        MapData mapData = row.getMap(4);
        assertThat(mapData).isNotNull();
        assertThat(mapData.size()).isEqualTo(2);
    }

    @Test
    void toRawEventsRow_setsIngestedAt_closeToNow() {
        Event event = makeEvent("evt-006", "websocket", "2024-01-15T00:00:00Z", new HashMap<>());
        long before = System.currentTimeMillis();
        RowData row = DeltaSinkFactory.toRawEventsRow(event);
        long after = System.currentTimeMillis();

        TimestampData ingestedAt = row.getTimestamp(5, 3);
        assertThat(ingestedAt).isNotNull();
        long ingestedMillis = ingestedAt.toLocalDateTime()
                .toInstant(java.time.ZoneOffset.UTC).toEpochMilli();
        assertThat(ingestedMillis)
                .isGreaterThanOrEqualTo(before)
                .isLessThanOrEqualTo(after + 100); // small tolerance
    }

    @Test
    void toRawEventsRow_handlesEmptyPayload() {
        Event event = makeEvent("evt-007", "rest_api", "2024-01-15T10:00:00Z", new HashMap<>());
        RowData row = DeltaSinkFactory.toRawEventsRow(event);
        MapData mapData = row.getMap(4);
        assertThat(mapData).isNotNull();
        assertThat(mapData.size()).isZero();
    }

    @Test
    void toRawEventsRow_fallsBackGracefullyOnInvalidTimestamp() {
        // Invalid timestamp string — factory falls back to Instant.now()
        Event event = makeEvent("evt-008", "rest_api", "NOT-A-TIMESTAMP", new HashMap<>());
        long before = System.currentTimeMillis();
        RowData row = DeltaSinkFactory.toRawEventsRow(event);
        long after = System.currentTimeMillis();

        TimestampData ts = row.getTimestamp(2, 3);
        assertThat(ts).isNotNull();
        long millis = ts.toLocalDateTime().toInstant(java.time.ZoneOffset.UTC).toEpochMilli();
        // Should be close to now
        assertThat(millis)
                .isGreaterThanOrEqualTo(before - 1000)
                .isLessThanOrEqualTo(after + 1000);
    }

    // -------------------------------------------------------------------------
    // Row conversion — toAggregationsRow
    // -------------------------------------------------------------------------

    @Test
    void toAggregationsRow_setsWindowStartAndEnd() {
        Instant start = Instant.parse("2024-01-15T12:00:00Z");
        Instant end   = Instant.parse("2024-01-15T12:01:00Z");
        AggregationResult r = makeAggResult(start, end, "rest_api", 5L, 100.0, 20.0);
        RowData row = DeltaSinkFactory.toAggregationsRow(r);

        TimestampData wsData = row.getTimestamp(0, 3);
        TimestampData weData = row.getTimestamp(1, 3);
        assertThat(wsData).isNotNull();
        assertThat(weData).isNotNull();
        assertThat(wsData.toLocalDateTime().toInstant(java.time.ZoneOffset.UTC))
                .isEqualTo(start);
        assertThat(weData.toLocalDateTime().toInstant(java.time.ZoneOffset.UTC))
                .isEqualTo(end);
    }

    @Test
    void toAggregationsRow_setsSource() {
        AggregationResult r = makeAggResult(Instant.now(), Instant.now(), "iot_simulator", 3L, 30.0, 10.0);
        RowData row = DeltaSinkFactory.toAggregationsRow(r);
        assertThat(row.getString(2).toString()).isEqualTo("iot_simulator");
    }

    @Test
    void toAggregationsRow_setsEventCount() {
        AggregationResult r = makeAggResult(Instant.now(), Instant.now(), "websocket", 42L, 0.0, 0.0);
        RowData row = DeltaSinkFactory.toAggregationsRow(r);
        assertThat(row.getLong(3)).isEqualTo(42L);
    }

    @Test
    void toAggregationsRow_setsMetricSumAndAvg() {
        AggregationResult r = makeAggResult(Instant.now(), Instant.now(), "rest_api", 4L, 200.0, 50.0);
        RowData row = DeltaSinkFactory.toAggregationsRow(r);
        assertThat(row.getDouble(4)).isCloseTo(200.0, within(1e-9));
        assertThat(row.getDouble(5)).isCloseTo(50.0, within(1e-9));
    }

    @Test
    void toAggregationsRow_setsWrittenAt() {
        Instant writtenAt = Instant.parse("2024-03-10T08:00:00Z");
        AggregationResult r = new AggregationResult(
                Instant.now(), Instant.now(), "rest_api", 1L, 1.0, 1.0, writtenAt);
        RowData row = DeltaSinkFactory.toAggregationsRow(r);

        TimestampData waData = row.getTimestamp(6, 3);
        assertThat(waData).isNotNull();
        assertThat(waData.toLocalDateTime().toInstant(java.time.ZoneOffset.UTC))
                .isEqualTo(writtenAt);
    }

    @Test
    void toAggregationsRow_handlesNullWindowBoundaries() {
        // AggregationResult may arrive with null window boundaries before WindowResultFunction
        AggregationResult r = new AggregationResult(null, null, "rest_api", 1L, 5.0, 5.0, Instant.now());
        RowData row = DeltaSinkFactory.toAggregationsRow(r);
        assertThat(row.isNullAt(0)).isTrue();
        assertThat(row.isNullAt(1)).isTrue();
    }

    // -------------------------------------------------------------------------
    // Sink instantiation smoke tests
    // -------------------------------------------------------------------------

    @Test
    void rawEventsSink_buildsWithoutException(@TempDir Path tempDir) {
        String path = tempDir.resolve("raw_events").toAbsolutePath().toString();
        DeltaSink<RowData> sink = DeltaSinkFactory.rawEventsSink(path);
        assertThat(sink).isNotNull();
    }

    @Test
    void aggregationsSink_buildsWithoutException(@TempDir Path tempDir) {
        String path = tempDir.resolve("aggregations").toAbsolutePath().toString();
        DeltaSink<RowData> sink = DeltaSinkFactory.aggregationsSink(path);
        assertThat(sink).isNotNull();
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private Event makeEvent(String id, String source, String timestamp, Map<String, String> payload) {
        return new Event(id, source, timestamp, payload);
    }

    private AggregationResult makeAggResult(Instant start, Instant end, String source,
                                            long count, double sum, double avg) {
        return new AggregationResult(start, end, source, count, sum, avg, Instant.now());
    }
}
