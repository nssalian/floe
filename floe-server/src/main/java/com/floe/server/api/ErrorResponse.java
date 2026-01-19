package com.floe.server.api;

import java.time.Instant;
import java.util.Map;

/** Standardized error response for all API endpoints. */
public record ErrorResponse(
        String error, String code, Map<String, Object> details, Instant timestamp) {
    /** Create an error response with just a message (derives code from message). */
    public static ErrorResponse of(String error) {
        return new ErrorResponse(error, null, Map.of(), Instant.now());
    }

    /** Create an error response with message and code. */
    public static ErrorResponse of(String error, String code) {
        return new ErrorResponse(error, code, Map.of(), Instant.now());
    }

    /** Create an error response with message, code, and details. */
    public static ErrorResponse of(String error, String code, Map<String, Object> details) {
        return new ErrorResponse(error, code, details != null ? details : Map.of(), Instant.now());
    }

    // Common error factory methods

    /** Resource not found error. */
    public static ErrorResponse notFound(String resourceType, String id) {
        return new ErrorResponse(
                resourceType + " not found: " + id,
                "NOT_FOUND",
                Map.of("resourceType", resourceType, "id", id),
                Instant.now());
    }

    /** Validation error. */
    public static ErrorResponse validation(String message) {
        return new ErrorResponse(message, "VALIDATION_FAILED", Map.of(), Instant.now());
    }

    /** Validation error with details. */
    public static ErrorResponse validation(String message, Map<String, Object> details) {
        return new ErrorResponse(
                message, "VALIDATION_FAILED", details != null ? details : Map.of(), Instant.now());
    }

    /** Conflict error (e.g., duplicate name). */
    public static ErrorResponse conflict(String message) {
        return new ErrorResponse(message, "CONFLICT", Map.of(), Instant.now());
    }

    /** Internal server error. */
    public static ErrorResponse internal(String message) {
        return new ErrorResponse(message, "INTERNAL_ERROR", Map.of(), Instant.now());
    }

    /** Internal server error from exception. */
    public static ErrorResponse internal(Exception e) {
        return new ErrorResponse(
                "Internal error: " + e.getMessage(), "INTERNAL_ERROR", Map.of(), Instant.now());
    }

    /** Unauthorized error. */
    public static ErrorResponse unauthorized(String message) {
        return new ErrorResponse(message, "UNAUTHORIZED", Map.of(), Instant.now());
    }

    /** Bad request error. */
    public static ErrorResponse badRequest(String message) {
        return new ErrorResponse(message, "BAD_REQUEST", Map.of(), Instant.now());
    }
}
