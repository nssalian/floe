package com.floe.server.scheduler;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;

/** Configuration for the Floe scheduler. */
@ConfigMapping(prefix = "floe.scheduler")
public interface SchedulerConfig {
    /**
     * Whether the scheduler is enabled.
     *
     * <p>When disabled, the scheduler poll loop exits immediately without processing policies.
     *
     * <p>Default: {@code true}
     */
    @WithDefault("true")
    boolean enabled();

    /**
     * Whether to use distributed locking for multi-replica deployments.
     *
     * <p>When enabled, the scheduler acquires a distributed lock before each poll cycle to prevent
     * duplicate executions.
     */
    @WithName("distributed-lock-enabled")
    @WithDefault("false")
    boolean distributedLockEnabled();
}
