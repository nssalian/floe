package com.floe.server.api;

import com.floe.core.health.HealthIssue;
import java.util.Locale;

/**
 * Health issue.
 *
 * @param type issue type
 * @param severity issue severity
 * @param message issue message
 */
public record HealthIssueResponse(String type, String severity, String message) {
    public static HealthIssueResponse from(HealthIssue issue) {
        return new HealthIssueResponse(
                issue.type().name(),
                issue.severity().name().toLowerCase(Locale.ROOT),
                issue.message());
    }
}
