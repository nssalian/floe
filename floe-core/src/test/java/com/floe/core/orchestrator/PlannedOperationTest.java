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
