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
