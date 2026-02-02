package com.floe.core.orchestrator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.floe.core.catalog.TableIdentifier;
import com.floe.core.health.HealthIssue;
import com.floe.core.health.HealthReport;
import com.floe.core.operation.OperationStats;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

class MaintenanceDebtScoreTest {

    @Test
    void calculateHealthyTableLowScore() {
        HealthReport report =
                HealthReport.builder(TableIdentifier.of("demo", "db", "table")).build();

        MaintenanceDebtScore score = MaintenanceDebtScore.calculate(report, null);

        assertTrue(score.score() < 1.0);
    }

    @Test
    void calculateCriticalIssuesHighScore() {
        HealthReport report =
                HealthReport.builder(TableIdentifier.of("demo", "db", "table"))
                        .issues(
                                List.of(
                                        HealthIssue.critical(
                                                HealthIssue.Type.TOO_MANY_SNAPSHOTS, "bad")))
                        .build();

        MaintenanceDebtScore score = MaintenanceDebtScore.calculate(report, null);

        assertTrue(score.score() >= 100.0);
    }

    @Test
    void calculateRecentFailuresIncreaseScore() {
        OperationStats stats =
                OperationStats.builder()
                        .totalOperations(3)
                        .failedCount(2)
                        .partialFailureCount(0)
                        .windowStart(Instant.now().minusSeconds(3600))
                        .windowEnd(Instant.now())
                        .consecutiveFailures(2)
                        .build();

        MaintenanceDebtScore score = MaintenanceDebtScore.calculate(null, stats);

        assertTrue(score.score() > 0.0);
    }

    @Test
    void calculateStaleMetadataIncreaseScore() {
        HealthReport report =
                HealthReport.builder(TableIdentifier.of("demo", "db", "table"))
                        .newestSnapshotAgeDays(20L)
                        .build();

        MaintenanceDebtScore score = MaintenanceDebtScore.calculate(report, null);

        assertTrue(score.score() > 0.0);
    }

    @Test
    void reasonBreakdownProvided() {
        HealthReport report =
                HealthReport.builder(TableIdentifier.of("demo", "db", "table"))
                        .issues(List.of(HealthIssue.warning(HealthIssue.Type.OLD_SNAPSHOTS, "old")))
                        .build();

        MaintenanceDebtScore score = MaintenanceDebtScore.calculate(report, null);

        assertTrue(score.breakdown().containsKey("healthIssues"));
    }

    @Test
    void calculateDeterministic() {
        HealthReport report =
                HealthReport.builder(TableIdentifier.of("demo", "db", "table"))
                        .issues(List.of(HealthIssue.warning(HealthIssue.Type.OLD_SNAPSHOTS, "old")))
                        .build();

        MaintenanceDebtScore a = MaintenanceDebtScore.calculate(report, null);
        MaintenanceDebtScore b = MaintenanceDebtScore.calculate(report, null);

        assertEquals(a.score(), b.score());
    }
}
