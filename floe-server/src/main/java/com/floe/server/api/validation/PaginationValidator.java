package com.floe.server.api.validation;

import com.floe.server.api.ErrorResponse;
import jakarta.ws.rs.core.Response;
import java.util.Optional;

/** Shared pagination validation utility. */
public final class PaginationValidator {

    /** Maximum allowed limit. */
    public static final int MAX_LIMIT = 100;

    /** Minimum allowed limit. */
    public static final int MIN_LIMIT = 1;

    private PaginationValidator() {
        // Utility class
    }

    /**
     * Validate pagination parameters.
     *
     * @param limit the requested limit
     * @param offset the requested offset
     * @return empty if valid, or a Response with error details if invalid
     */
    public static Optional<Response> validate(int limit, int offset) {
        if (limit < MIN_LIMIT || limit > MAX_LIMIT) {
            return Optional.of(
                    Response.status(Response.Status.BAD_REQUEST)
                            .entity(
                                    ErrorResponse.validation(
                                            "Limit must be between "
                                                    + MIN_LIMIT
                                                    + " and "
                                                    + MAX_LIMIT))
                            .build());
        }

        if (offset < 0) {
            return Optional.of(
                    Response.status(Response.Status.BAD_REQUEST)
                            .entity(ErrorResponse.validation("Offset must be non-negative"))
                            .build());
        }

        return Optional.empty();
    }
}
