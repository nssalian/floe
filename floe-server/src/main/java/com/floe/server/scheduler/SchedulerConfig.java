/*
 * Copyright 2026 The Floe Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

    /**
     * Maximum number of tables to process per poll cycle.
     *
     * <p>Limits the blast radius of each scheduler run. Set to 0 for unlimited.
     *
     * <p>Default: {@code 0} (unlimited)
     */
    @WithName("max-tables-per-poll")
    @WithDefault("0")
    int maxTablesPerPoll();

    /**
     * Maximum number of operations to execute per poll cycle.
     *
     * <p>Caps the total operations across all tables in a single poll. Set to 0 for unlimited.
     *
     * <p>Default: {@code 0} (unlimited)
     */
    @WithName("max-operations-per-poll")
    @WithDefault("0")
    int maxOperationsPerPoll();

    /**
     * Maximum bytes to rewrite per hour.
     *
     * <p>Limits the amount of data rewritten to prevent overwhelming storage systems. Set to 0 for
     * unlimited. Uses the normalized bytesRewritten metric from operations.
     *
     * <p>Default: {@code 0} (unlimited)
     */
    @WithName("max-bytes-per-hour")
    @WithDefault("0")
    long maxBytesPerHour();

    /**
     * Number of consecutive failures before backoff kicks in.
     *
     * <p>Default: {@code 3}
     */
    @WithName("failure-backoff-threshold")
    @WithDefault("3")
    int failureBackoffThreshold();

    /**
     * Backoff duration in hours after repeated failures.
     *
     * <p>Default: {@code 6}
     */
    @WithName("failure-backoff-hours")
    @WithDefault("6")
    int failureBackoffHours();

    /**
     * Number of consecutive zero-change runs before reducing frequency.
     *
     * <p>Default: {@code 5}
     */
    @WithName("zero-change-threshold")
    @WithDefault("5")
    int zeroChangeThreshold();

    /**
     * Percentage to reduce frequency by when zero-change threshold is exceeded.
     *
     * <p>Default: {@code 50}
     */
    @WithName("zero-change-frequency-reduction-percent")
    @WithDefault("50")
    int zeroChangeFrequencyReductionPercent();

    /**
     * Minimum interval in hours when reducing frequency for zero-change runs.
     *
     * <p>Default: {@code 6}
     */
    @WithName("zero-change-min-interval-hours")
    @WithDefault("6")
    int zeroChangeMinIntervalHours();

    /**
     * Whether condition-based triggering is enabled.
     *
     * <p>When enabled, policies with triggerConditions are evaluated against table health before
     * operations execute. When disabled, triggerConditions are ignored and operations run purely on
     * schedule (cron-based behavior).
     *
     * <p>Default: {@code true}
     */
    @WithName("condition-based-triggering-enabled")
    @WithDefault("true")
    boolean conditionBasedTriggeringEnabled();
}
