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

package com.floe.server.api;

import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotBlank;

/**
 * Request to trigger policy-driven maintenance for a table.
 *
 * @param catalog catalog name
 * @param namespace namespace name
 * @param table table name
 * @param policyName optional specific policy to use
 */
public record TriggerRequest(
        @NotBlank(message = "catalog is required") String catalog,
        @NotBlank(message = "namespace is required") String namespace,
        @NotBlank(message = "table is required") String table,
        @Nullable String policyName) {
    /** Check if a specific policy was requested. */
    public boolean hasSpecificPolicy() {
        return policyName != null && !policyName.isBlank();
    }
}
