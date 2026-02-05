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

package com.floe.core.scheduler;

import java.time.Instant;

/**
 * Record of when a policy/operation was last executed for scheduling purposes.
 *
 * @param policyId The policy identifier
 * @param operationType The operation type (REWRITE_DATA_FILES, EXPIRE_SNAPSHOTS, etc.)
 * @param tableKey The fully qualified table key (catalog.namespace.table)
 * @param lastRunAt When the operation was last triggered
 * @param nextRunAt When the operation should next run (computed from schedule)
 */
public record ScheduleExecutionRecord(
        String policyId,
        String operationType,
        String tableKey,
        Instant lastRunAt,
        Instant nextRunAt) {

    /** Create a composite key for this execution record. */
    public String compositeKey() {
        return policyId + ":" + operationType + ":" + tableKey;
    }

    /** Check if this operation is due for execution. */
    public boolean isDue() {
        return isDue(Instant.now());
    }

    /** Check if this operation is due for execution at the given time. */
    public boolean isDue(Instant atTime) {
        if (nextRunAt == null) {
            return true; // Never run, due immediately
        }
        return !atTime.isBefore(nextRunAt);
    }

    /** Create a new record with updated run times. */
    public ScheduleExecutionRecord withNewRun(Instant runAt, Instant nextRun) {
        return new ScheduleExecutionRecord(policyId, operationType, tableKey, runAt, nextRun);
    }

    /** Builder for ScheduleExecutionRecord. */
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String policyId;
        private String operationType;
        private String tableKey;
        private Instant lastRunAt;
        private Instant nextRunAt;

        public Builder policyId(String policyId) {
            this.policyId = policyId;
            return this;
        }

        public Builder operationType(String operationType) {
            this.operationType = operationType;
            return this;
        }

        public Builder tableKey(String tableKey) {
            this.tableKey = tableKey;
            return this;
        }

        public Builder lastRunAt(Instant lastRunAt) {
            this.lastRunAt = lastRunAt;
            return this;
        }

        public Builder nextRunAt(Instant nextRunAt) {
            this.nextRunAt = nextRunAt;
            return this;
        }

        public ScheduleExecutionRecord build() {
            return new ScheduleExecutionRecord(
                    policyId, operationType, tableKey, lastRunAt, nextRunAt);
        }
    }
}
