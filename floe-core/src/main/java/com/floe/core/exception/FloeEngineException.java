package com.floe.core.exception;

/**
 * Exception thrown when an execution engine operation fails.
 *
 * <p>Used for Spark/Livy submission errors, Trino query failures, and similar engine issues.
 */
public class FloeEngineException extends RuntimeException {

    private final String engineType;
    private final String operation;
    private final String jobId;

    public FloeEngineException(String engineType, String operation, String jobId, String details) {
        super(buildMessage(engineType, operation, jobId, details));
        this.engineType = engineType;
        this.operation = operation;
        this.jobId = jobId;
    }

    private static String buildMessage(
            String engineType, String operation, String jobId, String details) {
        StringBuilder msg = new StringBuilder();
        msg.append(engineType).append(" engine ").append(operation).append(" failed");
        if (jobId != null) {
            msg.append(" for job ").append(jobId);
        }
        if (details != null) {
            msg.append(": ").append(details);
        }
        return msg.toString();
    }

    /** The operation that failed (e.g., "submit", "poll", "cancel"). */
    public String getOperation() {
        return operation;
    }
}
