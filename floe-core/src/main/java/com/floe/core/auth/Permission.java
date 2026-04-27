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

package com.floe.core.auth;

/**
 * Granular permissions for API access control.
 *
 * <p>Permissions are grouped by resource type and can be extended as needed.
 */
public enum Permission {
    // Policy management
    READ_POLICIES("Read maintenance policies"),
    WRITE_POLICIES("Create and update maintenance policies"),
    DELETE_POLICIES("Delete maintenance policies"),

    // Table operations
    READ_TABLES("View tables and their metadata"),

    // Maintenance operations
    READ_OPERATIONS("View maintenance operation history"),
    TRIGGER_MAINTENANCE("Trigger maintenance operations"),

    // API key management
    MANAGE_API_KEYS("Create, update, and revoke API keys");

    private final String description;

    Permission(String description) {
        this.description = description;
    }

    public String description() {
        return description;
    }
}
