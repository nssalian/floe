package com.floe.core.orchestrator;

import com.floe.core.health.HealthIssue;
import com.floe.core.health.HealthReport;
import com.floe.core.operation.OperationStats;
import java.util.HashMap;
import java.util.Map;

/** Computes a maintenance debt score for a table based on health and recent outcomes. */
public record MaintenanceDebtScore(double score, Map<String, Double> breakdown)
        implements Comparable<MaintenanceDebtScore> {
    private static final double MAX_SCORE = 1000.0;

    public static MaintenanceDebtScore calculate(HealthReport report, OperationStats stats) {
        Map<String, Double> breakdown = new HashMap<>();
        double score = 0.0;

        if (report != null && report.issues() != null) {
            double issueScore = 0.0;
            for (HealthIssue issue : report.issues()) {
                issueScore +=
                        switch (issue.severity()) {
                            case CRITICAL -> 100.0;
                            case WARNING -> 50.0;
                            case INFO -> 10.0;
                        };
            }
            if (issueScore > 0) {
                breakdown.put("healthIssues", issueScore);
                score += issueScore;
            }

            Long newestAge = report.newestSnapshotAgeDays();
            if (newestAge != null && newestAge > 0) {
                double ageScore = newestAge * 0.5;
                breakdown.put("staleMetadata", ageScore);
                score += ageScore;
            }
        }

        if (stats != null) {
            double failureRate = stats.failureRate();
            if (failureRate > 0) {
                double failureScore = failureRate;
                breakdown.put("failureRate", failureScore);
                score += failureScore;
            }

            if (stats.consecutiveFailures() > 0) {
                double failureStreak = stats.consecutiveFailures() * 20.0;
                breakdown.put("failureStreak", failureStreak);
                score += failureStreak;
            }
        }

        if (score > MAX_SCORE) {
            score = MAX_SCORE;
        }

        return new MaintenanceDebtScore(score, breakdown);
    }

    @Override
    public int compareTo(MaintenanceDebtScore other) {
        return Double.compare(this.score, other.score);
    }
}
