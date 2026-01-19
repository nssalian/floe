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
