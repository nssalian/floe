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

package com.floe.core.orchestrator;

import static org.assertj.core.api.Assertions.assertThat;

import com.floe.core.health.HealthIssue;
import com.floe.core.maintenance.MaintenanceOperation;
import org.junit.jupiter.api.Test;

class PlannedOperationTest {

    @Test
    void plannedOperationHasOperationType() {
        PlannedOperation op =
                PlannedOperation.critical(MaintenanceOperation.Type.REWRITE_DATA_FILES, "reason");

        assertThat(op.operationType()).isEqualTo(MaintenanceOperation.Type.REWRITE_DATA_FILES);
    }

    @Test
    void plannedOperationHasReason() {
        PlannedOperation op =
                PlannedOperation.warning(MaintenanceOperation.Type.EXPIRE_SNAPSHOTS, "reason");

        assertThat(op.reason()).isEqualTo("reason");
    }

    @Test
    void plannedOperationHasSeverity() {
        PlannedOperation op =
                PlannedOperation.info(MaintenanceOperation.Type.ORPHAN_CLEANUP, "reason");

        assertThat(op.severity()).isEqualTo(HealthIssue.Severity.INFO);
    }
}
