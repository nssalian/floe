package com.floe.core.exception;

/**
 * Exception thrown when a database operation fails.
 *
 * <p>Wraps SQL exceptions and other data access errors from Postgres stores.
 */
public class FloeDataAccessException extends RuntimeException {

    private final String operation;
    private final String entity;

    public FloeDataAccessException(String operation, String entity, Throwable cause) {
        super(buildMessage(operation, entity, cause), cause);
        this.operation = operation;
        this.entity = entity;
    }

    public FloeDataAccessException(
            String operation, String entity, String details, Throwable cause) {
        super(buildMessage(operation, entity, details), cause);
        this.operation = operation;
        this.entity = entity;
    }

    private static String buildMessage(String operation, String entity, Throwable cause) {
        String msg = String.format("Database %s failed for %s", operation, entity);
        if (cause != null && cause.getMessage() != null) {
            msg += ": " + cause.getMessage();
        }
        return msg;
    }

    private static String buildMessage(String operation, String entity, String details) {
        return String.format("Database %s failed for %s: %s", operation, entity, details);
    }

    /** The database operation that failed (e.g., "insert", "update", "query"). */
    public String getOperation() {
        return operation;
    }
}
