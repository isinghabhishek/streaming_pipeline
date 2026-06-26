package com.pipeline.flink;

import com.pipeline.flink.health.MetricsRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for {@link MetricsRegistry}.
 * Verifies counter increments and Prometheus text export format.
 */
class MetricsRegistryTest {

    @BeforeEach
    void resetCounters() {
        // Reset all counters to zero before each test
        MetricsRegistry.recordsConsumed.reset();
        MetricsRegistry.recordsWritten.reset();
        MetricsRegistry.parseErrors.reset();
        MetricsRegistry.dlqEvents.reset();
        MetricsRegistry.checkpointDurationMs.set(0L);
        MetricsRegistry.processingLagMs.set(0L);
    }

    @Test
    void counters_startAtZero() {
        assertThat(MetricsRegistry.recordsConsumed.sum()).isZero();
        assertThat(MetricsRegistry.recordsWritten.sum()).isZero();
        assertThat(MetricsRegistry.parseErrors.sum()).isZero();
        assertThat(MetricsRegistry.dlqEvents.sum()).isZero();
    }

    @Test
    void incRecordsConsumed_incrementsCounter() {
        MetricsRegistry.incRecordsConsumed();
        MetricsRegistry.incRecordsConsumed();
        assertThat(MetricsRegistry.recordsConsumed.sum()).isEqualTo(2L);
    }

    @Test
    void incParseErrors_incrementsCounter() {
        MetricsRegistry.incParseErrors();
        assertThat(MetricsRegistry.parseErrors.sum()).isEqualTo(1L);
    }

    @Test
    void setCheckpointDuration_updatesGauge() {
        MetricsRegistry.setCheckpointDuration(1234L);
        assertThat(MetricsRegistry.checkpointDurationMs.get()).isEqualTo(1234L);
    }

    @Test
    void toPrometheusText_containsAllMetricNames() {
        String output = MetricsRegistry.toPrometheusText();
        assertThat(output).contains("pipeline_records_consumed_total");
        assertThat(output).contains("pipeline_records_written_total");
        assertThat(output).contains("pipeline_parse_errors_total");
        assertThat(output).contains("pipeline_dlq_events_total");
        assertThat(output).contains("pipeline_checkpoint_duration_ms");
        assertThat(output).contains("pipeline_processing_lag_ms");
    }

    @Test
    void toPrometheusText_containsTypeAnnotations() {
        String output = MetricsRegistry.toPrometheusText();
        assertThat(output).contains("# TYPE pipeline_records_consumed_total counter");
        assertThat(output).contains("# TYPE pipeline_checkpoint_duration_ms gauge");
    }

    @Test
    void toPrometheusText_reflectsCurrentCounterValues() {
        MetricsRegistry.incRecordsConsumed();
        MetricsRegistry.incRecordsConsumed();
        MetricsRegistry.incParseErrors();
        MetricsRegistry.setCheckpointDuration(500L);

        String output = MetricsRegistry.toPrometheusText();

        assertThat(output).contains("pipeline_records_consumed_total 2");
        assertThat(output).contains("pipeline_parse_errors_total 1");
        assertThat(output).contains("pipeline_checkpoint_duration_ms 500");
    }
}
