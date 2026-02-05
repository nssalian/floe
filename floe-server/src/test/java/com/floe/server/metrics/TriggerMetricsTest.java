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

package com.floe.server.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for trigger evaluation metrics in FloeMetrics. */
class TriggerMetricsTest {

    private MeterRegistry registry;
    private FloeMetrics metrics;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        metrics = new FloeMetrics(registry);
    }

    @Test
    void recordTriggerEvaluationIncrementsTotalCounter() {
        metrics.recordTriggerEvaluation("COMPACTION", true, false, false);
        metrics.recordTriggerEvaluation("EXPIRE_SNAPSHOTS", false, false, false);

        Counter counter = registry.find("floe_trigger_evaluations_total").counter();
        assertThat(counter).isNotNull();
        assertThat(counter.count()).isEqualTo(2.0);
    }

    @Test
    void recordTriggerEvaluationTracksConditionsMet() {
        metrics.recordTriggerEvaluation("COMPACTION", true, false, false);
        metrics.recordTriggerEvaluation("COMPACTION", true, false, false);

        Counter metCounter = registry.find("floe_trigger_conditions_met_total").counter();
        assertThat(metCounter).isNotNull();
        assertThat(metCounter.count()).isEqualTo(2.0);

        Counter notMetCounter = registry.find("floe_trigger_conditions_not_met_total").counter();
        assertThat(notMetCounter).isNotNull();
        assertThat(notMetCounter.count()).isEqualTo(0.0);
    }

    @Test
    void recordTriggerEvaluationTracksConditionsNotMet() {
        metrics.recordTriggerEvaluation("COMPACTION", false, false, false);

        Counter metCounter = registry.find("floe_trigger_conditions_met_total").counter();
        assertThat(metCounter).isNotNull();
        assertThat(metCounter.count()).isEqualTo(0.0);

        Counter notMetCounter = registry.find("floe_trigger_conditions_not_met_total").counter();
        assertThat(notMetCounter).isNotNull();
        assertThat(notMetCounter.count()).isEqualTo(1.0);
    }

    @Test
    void recordTriggerEvaluationTracksBlockedByInterval() {
        metrics.recordTriggerEvaluation("COMPACTION", false, true, false);
        metrics.recordTriggerEvaluation("EXPIRE_SNAPSHOTS", false, true, false);

        Counter counter = registry.find("floe_trigger_blocked_by_interval_total").counter();
        assertThat(counter).isNotNull();
        assertThat(counter.count()).isEqualTo(2.0);
    }

    @Test
    void recordTriggerEvaluationTracksForcedByCriticalPipeline() {
        metrics.recordTriggerEvaluation("COMPACTION", true, false, true);

        Counter counter = registry.find("floe_trigger_forced_by_critical_pipeline_total").counter();
        assertThat(counter).isNotNull();
        assertThat(counter.count()).isEqualTo(1.0);
    }

    @Test
    void recordTriggerEvaluationTracksByOperationType() {
        metrics.recordTriggerEvaluation("COMPACTION", true, false, false);
        metrics.recordTriggerEvaluation("COMPACTION", false, false, false);
        metrics.recordTriggerEvaluation("EXPIRE_SNAPSHOTS", true, false, false);

        Counter compactionTriggered =
                registry.find("floe_trigger_evaluations_by_operation_total")
                        .tag("operation", "compaction")
                        .tag("result", "triggered")
                        .counter();
        assertThat(compactionTriggered).isNotNull();
        assertThat(compactionTriggered.count()).isEqualTo(1.0);

        Counter compactionSkipped =
                registry.find("floe_trigger_evaluations_by_operation_total")
                        .tag("operation", "compaction")
                        .tag("result", "skipped")
                        .counter();
        assertThat(compactionSkipped).isNotNull();
        assertThat(compactionSkipped.count()).isEqualTo(1.0);

        Counter expireTriggered =
                registry.find("floe_trigger_evaluations_by_operation_total")
                        .tag("operation", "expire_snapshots")
                        .tag("result", "triggered")
                        .counter();
        assertThat(expireTriggered).isNotNull();
        assertThat(expireTriggered.count()).isEqualTo(1.0);
    }

    @Test
    void recordTriggerConditionsMetUsesConvenienceMethod() {
        metrics.recordTriggerConditionsMet("COMPACTION", false);

        Counter totalCounter = registry.find("floe_trigger_evaluations_total").counter();
        assertThat(totalCounter.count()).isEqualTo(1.0);

        Counter metCounter = registry.find("floe_trigger_conditions_met_total").counter();
        assertThat(metCounter.count()).isEqualTo(1.0);

        Counter forcedCounter =
                registry.find("floe_trigger_forced_by_critical_pipeline_total").counter();
        assertThat(forcedCounter.count()).isEqualTo(0.0);
    }

    @Test
    void recordTriggerConditionsMetWithCriticalPipeline() {
        metrics.recordTriggerConditionsMet("COMPACTION", true);

        Counter forcedCounter =
                registry.find("floe_trigger_forced_by_critical_pipeline_total").counter();
        assertThat(forcedCounter.count()).isEqualTo(1.0);
    }

    @Test
    void recordTriggerBlockedByIntervalUsesConvenienceMethod() {
        metrics.recordTriggerBlockedByInterval("COMPACTION");

        Counter totalCounter = registry.find("floe_trigger_evaluations_total").counter();
        assertThat(totalCounter.count()).isEqualTo(1.0);

        Counter blockedCounter = registry.find("floe_trigger_blocked_by_interval_total").counter();
        assertThat(blockedCounter.count()).isEqualTo(1.0);

        Counter notMetCounter = registry.find("floe_trigger_conditions_not_met_total").counter();
        assertThat(notMetCounter.count()).isEqualTo(1.0);
    }

    @Test
    void recordTriggerConditionsNotMetUsesConvenienceMethod() {
        metrics.recordTriggerConditionsNotMet("EXPIRE_SNAPSHOTS");

        Counter totalCounter = registry.find("floe_trigger_evaluations_total").counter();
        assertThat(totalCounter.count()).isEqualTo(1.0);

        Counter notMetCounter = registry.find("floe_trigger_conditions_not_met_total").counter();
        assertThat(notMetCounter.count()).isEqualTo(1.0);

        Counter blockedCounter = registry.find("floe_trigger_blocked_by_interval_total").counter();
        assertThat(blockedCounter.count()).isEqualTo(0.0);
    }

    @Test
    void operationTypeIsLowercasedInTags() {
        metrics.recordTriggerEvaluation("REWRITE_MANIFESTS", true, false, false);

        Counter counter =
                registry.find("floe_trigger_evaluations_by_operation_total")
                        .tag("operation", "rewrite_manifests")
                        .counter();
        assertThat(counter).isNotNull();
        assertThat(counter.count()).isEqualTo(1.0);
    }
}
