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
