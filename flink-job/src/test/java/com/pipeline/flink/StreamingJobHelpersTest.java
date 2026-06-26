package com.pipeline.flink;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for the pure helper methods on {@link StreamingJob} —
 * env-var reading, window clamping.
 * No Flink runtime required.
 */
class StreamingJobHelpersTest {

    @Test
    void envOrDefault_returnsDefaultWhenEnvAbsent() {
        String result = StreamingJob.envOrDefault("__NO_SUCH_VAR__", "fallback");
        assertThat(result).isEqualTo("fallback");
    }

    @Test
    void envLong_returnsDefaultWhenEnvAbsent() {
        long result = StreamingJob.envLong("__NO_SUCH_VAR__", 42L);
        assertThat(result).isEqualTo(42L);
    }

    @Test
    void clampWindow_clampsBelow10() {
        assertThat(StreamingJob.clampWindow(5L)).isEqualTo(10L);
        assertThat(StreamingJob.clampWindow(0L)).isEqualTo(10L);
    }

    @Test
    void clampWindow_clampsAbove300() {
        assertThat(StreamingJob.clampWindow(500L)).isEqualTo(300L);
        assertThat(StreamingJob.clampWindow(301L)).isEqualTo(300L);
    }

    @Test
    void clampWindow_allowsValidRange() {
        assertThat(StreamingJob.clampWindow(10L)).isEqualTo(10L);
        assertThat(StreamingJob.clampWindow(60L)).isEqualTo(60L);
        assertThat(StreamingJob.clampWindow(300L)).isEqualTo(300L);
    }
}
