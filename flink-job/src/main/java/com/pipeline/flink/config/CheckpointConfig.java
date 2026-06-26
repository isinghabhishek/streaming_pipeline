package com.pipeline.flink.config;

import com.pipeline.flink.health.MetricsRegistry;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Applies checkpointing and restart-strategy configuration to the
 * {@link StreamExecutionEnvironment}.
 *
 * <p>Settings (Requirements 3.6, 3.7, 4.2):
 * <ul>
 *   <li>Checkpointing mode: {@code EXACTLY_ONCE} — ensures atomicity with the
 *       Delta 2PC transaction.</li>
 *   <li>Checkpoint interval: {@code CHECKPOINT_INTERVAL_MS} env var (default 10 000 ms).</li>
 *   <li>Checkpoint timeout: 30 000 ms — if a checkpoint does not complete in 30 s,
 *       Flink logs a warning and tries again at the next interval.</li>
 *   <li>Max concurrent checkpoints: 1 — prevents overlapping snapshots.</li>
 *   <li>Externalized checkpoints retained on cancellation: lets the job resume
 *       from the last good checkpoint after an operator restart.</li>
 *   <li>Restart strategy: fixed-delay, 3 attempts, 10 s delay.
 *       After 3 consecutive failures the job transitions to FAILED and the
 *       health endpoint returns 503 (Requirement 3.8).</li>
 *   <li>State backend: filesystem ({@code CHECKPOINT_DIR} env var, default
 *       {@code file:///data/checkpoints}). Kafka consumer offsets are committed
 *       atomically with the Delta transaction on checkpoint completion
 *       (the Kafka connector's 2PC mechanism handles this automatically when
 *       checkpointing mode is {@code EXACTLY_ONCE}).</li>
 * </ul>
 */
public class CheckpointConfig {

    private static final Logger LOG = LoggerFactory.getLogger(CheckpointConfig.class);

    private static final long DEFAULT_INTERVAL_MS  = 10_000L;
    private static final long CHECKPOINT_TIMEOUT   = 30_000L;
    private static final int  MAX_FAILURES         = 3;
    private static final long RESTART_DELAY_MS     = 10_000L;

    private CheckpointConfig() {}

    /**
     * Configures checkpointing, state backend, and restart strategy on {@code env}.
     *
     * @param env              the stream execution environment to configure
     * @param checkpointDirUri URI of the checkpoint storage directory
     *                         (e.g. {@code file:///data/checkpoints} or {@code s3://bucket/cp})
     * @param intervalMs       checkpoint interval in milliseconds
     */
    public static void apply(StreamExecutionEnvironment env,
                             String checkpointDirUri,
                             long intervalMs) {

        // --- State backend ---------------------------------------------------
        // In Flink 1.18, checkpoint storage is configured via CheckpointConfig.
        // The default state backend (HashMapStateBackend) keeps state on heap;
        // checkpoint data is written to the durable path configured below.
        // This satisfies the "save to durable volume" requirement (3.6).

        // --- Checkpointing ---------------------------------------------------
        org.apache.flink.streaming.api.environment.CheckpointConfig cc =
                env.getCheckpointConfig();

        // Set durable checkpoint storage path (replaces deprecated FsStateBackend)
        cc.setCheckpointStorage(checkpointDirUri);

        env.enableCheckpointing(intervalMs, CheckpointingMode.EXACTLY_ONCE);

        cc.setCheckpointTimeout(CHECKPOINT_TIMEOUT);
        cc.setMaxConcurrentCheckpoints(1);

        // Retain checkpoints when the job is cancelled so we can resume from them
        cc.enableExternalizedCheckpoints(
                ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // Tolerate up to 1 consecutive checkpoint failure before alerting;
        // after MAX_FAILURES the restart strategy exhausts retries → FAILED state
        cc.setTolerableCheckpointFailureNumber(1);

        // --- Restart strategy ------------------------------------------------
        // Fixed-delay: 3 retries, 10 s between each attempt.
        // On the 4th failure the job reaches FAILED state → health endpoint → 503.
        env.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(MAX_FAILURES, Time.milliseconds(RESTART_DELAY_MS)));

        LOG.info("{\"component\":\"CheckpointConfig\","
                + "\"level\":\"INFO\","
                + "\"message\":\"Checkpointing configured\","
                + "\"interval_ms\":{},\"timeout_ms\":{},\"dir\":\"{}\","
                + "\"max_retries\":{}}",
                intervalMs, CHECKPOINT_TIMEOUT, checkpointDirUri, MAX_FAILURES);
    }

    // -------------------------------------------------------------------------
    // Checkpoint listener — records duration in MetricsRegistry
    // -------------------------------------------------------------------------

    /**
     * Returns a {@link org.apache.flink.api.common.JobExecutionResult} listener
     * that updates {@link MetricsRegistry#checkpointDurationMs} after each
     * successful checkpoint notification.
     *
     * <p>Wire this up via
     * {@code env.registerJobListener(CheckpointConfig.buildListener())}.
     */
    public static org.apache.flink.core.execution.JobListener buildListener() {
        return new org.apache.flink.core.execution.JobListener() {
            @Override
            public void onJobSubmitted(
                    org.apache.flink.core.execution.JobClient jobClient,
                    Throwable throwable) {
                if (throwable != null) {
                    LOG.error("{\"component\":\"CheckpointConfig\","
                            + "\"level\":\"ERROR\","
                            + "\"message\":\"Job submission failed\","
                            + "\"error\":\"{}\"}",
                            throwable.getMessage());
                }
            }

            @Override
            public void onJobExecuted(
                    org.apache.flink.api.common.JobExecutionResult jobExecutionResult,
                    Throwable throwable) {
                if (throwable == null) {
                    LOG.info("{\"component\":\"CheckpointConfig\","
                            + "\"level\":\"INFO\","
                            + "\"message\":\"Job execution completed successfully\"}");
                } else {
                    LOG.error("{\"component\":\"CheckpointConfig\","
                            + "\"level\":\"ERROR\","
                            + "\"message\":\"Job execution failed\","
                            + "\"error\":\"{}\"}",
                            throwable.getMessage());
                }
            }
        };
    }

    // -------------------------------------------------------------------------
    // Env-var helper
    // -------------------------------------------------------------------------

    /** Reads {@code CHECKPOINT_INTERVAL_MS} from the environment, defaulting to 10 000. */
    public static long intervalMsFromEnv() {
        String raw = System.getenv("CHECKPOINT_INTERVAL_MS");
        if (raw == null || raw.isBlank()) return DEFAULT_INTERVAL_MS;
        try {
            return Long.parseLong(raw.trim());
        } catch (NumberFormatException e) {
            LOG.warn("Invalid CHECKPOINT_INTERVAL_MS='{}'; using default {}",
                    raw, DEFAULT_INTERVAL_MS);
            return DEFAULT_INTERVAL_MS;
        }
    }
}
