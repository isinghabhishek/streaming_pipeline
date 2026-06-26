package com.pipeline.flink.sink;

import com.pipeline.flink.model.AggregationResult;
import com.pipeline.flink.model.Event;
import io.delta.flink.sink.DeltaSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.hadoop.conf.Configuration;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;

/**
 * Factory that builds {@link DeltaSink} instances for the {@code raw_events}
 * and {@code aggregations} Delta Lake tables.
 *
 * <p>The Delta-Flink connector uses two-phase commit (2PC) backed by Flink
 * checkpoints to provide exactly-once write semantics:
 * <ol>
 *   <li>On checkpoint start, the sink pre-commits (snaps) pending Parquet files.</li>
 *   <li>On checkpoint completion, the sink commits the Delta transaction log entry
 *       atomically. If the job fails between snap and commit, the uncommitted
 *       files are rolled back on the next recovery.</li>
 * </ol>
 *
 * <p>Partition columns match the Delta Lake schema defined in design.md:
 * <ul>
 *   <li>{@code raw_events}   — partitioned by {@code source} and {@code date}</li>
 *   <li>{@code aggregations} — partitioned by {@code source} and {@code window_start}</li>
 * </ul>
 *
 * <p>Requirements: 3.5, 4.1, 4.3, 5.1, 5.2
 */
public class DeltaSinkFactory {

    private DeltaSinkFactory() {}

    // -------------------------------------------------------------------------
    // raw_events table
    // -------------------------------------------------------------------------

    /**
     * RowType schema for the {@code raw_events} Delta table.
     *
     * Columns: event_id, source, timestamp, date (partition), payload, ingested_at
     */
    public static RowType rawEventsRowType() {
        return RowType.of(
                new VarCharType(VarCharType.MAX_LENGTH),   // event_id
                new VarCharType(VarCharType.MAX_LENGTH),   // source
                new TimestampType(3),                       // timestamp
                new VarCharType(10),                        // date  (yyyy-MM-dd, partition col)
                new MapType(                                // payload map<string,string>
                        new VarCharType(VarCharType.MAX_LENGTH),
                        new VarCharType(VarCharType.MAX_LENGTH)),
                new TimestampType(3)                        // ingested_at
        );
    }

    /**
     * Builds the {@link DeltaSink} for the {@code raw_events} table.
     *
     * @param deltaTablePath  absolute path to the Delta table directory (e.g. /data/delta/raw_events)
     */
    public static DeltaSink<RowData> rawEventsSink(String deltaTablePath) {
        Configuration hadoopConf = new Configuration();
        return DeltaSink
                .forRowData(new Path(deltaTablePath), hadoopConf, rawEventsRowType())
                .withPartitionColumns("source", "date")
                .build();
    }

    /**
     * Converts an {@link Event} to a {@link GenericRowData} matching {@link #rawEventsRowType()}.
     */
    public static RowData toRawEventsRow(Event event) {
        GenericRowData row = new GenericRowData(6);

        row.setField(0, StringData.fromString(event.getEventId()));
        row.setField(1, StringData.fromString(event.getSource()));

        // Parse ISO-8601 timestamp to TimestampData
        TimestampData ts = parseTimestamp(event.getTimestamp());
        row.setField(2, ts);

        // Derive date partition column (yyyy-MM-dd)
        String date = ts != null
                ? ts.toLocalDateTime().toLocalDate().toString()
                : LocalDate.now(ZoneOffset.UTC).toString();
        row.setField(3, StringData.fromString(date));

        // Convert payload map
        Map<StringData, StringData> payloadMap = new HashMap<>();
        event.getPayload().forEach((k, v) ->
                payloadMap.put(StringData.fromString(k), StringData.fromString(v)));
        row.setField(4, new GenericMapData(payloadMap));

        // ingested_at = now
        row.setField(5, TimestampData.fromInstant(java.time.Instant.now()));

        return row;
    }

    // -------------------------------------------------------------------------
    // aggregations table
    // -------------------------------------------------------------------------

    /**
     * RowType schema for the {@code aggregations} Delta table.
     *
     * Columns: window_start (partition), window_end, source (partition),
     *          event_count, metric_sum, metric_avg, written_at
     */
    public static RowType aggregationsRowType() {
        return RowType.of(
                new TimestampType(3),                       // window_start  (partition col)
                new TimestampType(3),                       // window_end
                new VarCharType(VarCharType.MAX_LENGTH),   // source        (partition col)
                new BigIntType(),                           // event_count
                new DoubleType(),                           // metric_sum
                new DoubleType(),                           // metric_avg
                new TimestampType(3)                        // written_at
        );
    }

    /**
     * Builds the {@link DeltaSink} for the {@code aggregations} table.
     */
    public static DeltaSink<RowData> aggregationsSink(String deltaTablePath) {
        Configuration hadoopConf = new Configuration();
        return DeltaSink
                .forRowData(new Path(deltaTablePath), hadoopConf, aggregationsRowType())
                .withPartitionColumns("source", "window_start")
                .build();
    }

    /**
     * Converts an {@link AggregationResult} to a {@link GenericRowData}
     * matching {@link #aggregationsRowType()}.
     */
    public static RowData toAggregationsRow(AggregationResult r) {
        GenericRowData row = new GenericRowData(7);
        row.setField(0, r.getWindowStart() != null
                ? TimestampData.fromInstant(r.getWindowStart()) : null);
        row.setField(1, r.getWindowEnd() != null
                ? TimestampData.fromInstant(r.getWindowEnd()) : null);
        row.setField(2, StringData.fromString(r.getSource()));
        row.setField(3, r.getEventCount());
        row.setField(4, r.getMetricSum());
        row.setField(5, r.getMetricAvg());
        row.setField(6, r.getWrittenAt() != null
                ? TimestampData.fromInstant(r.getWrittenAt()) : null);
        return row;
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static TimestampData parseTimestamp(String iso8601) {
        try {
            return TimestampData.fromInstant(java.time.Instant.parse(iso8601));
        } catch (Exception e) {
            return TimestampData.fromInstant(java.time.Instant.now());
        }
    }
}
