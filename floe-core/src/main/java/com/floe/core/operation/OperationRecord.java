package com.floe.core.operation;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Record of a maintenance operation run, persisted for audit and UI display.
 *
 * @param id unique identifier
 * @param catalog catalog name
 * @param namespace namespace name
 * @param tableName table name
 * @param policyName policy that triggered this operation
 * @param policyId policy ID
 * @param status current status
 * @param startedAt when operation started
 * @param completedAt when operation completed
 * @param results operation results
 * @param errorMessage error message if failed
 * @param createdAt when record was created
 */
public record OperationRecord(
        UUID id,
        String catalog,
        String namespace,
        String tableName,
        String policyName,
        UUID policyId,
        OperationStatus status,
        Instant startedAt,
        Instant completedAt,
        OperationResults results,
        String errorMessage,
        Instant createdAt) {
    /** Fully qualified table name: catalog.namespace.table */
    public String qualifiedTableName() {
        return catalog + "." + namespace + "." + tableName;
    }

    /** Duration of the operation, or time since start if still running. */
    public Duration duration() {
        if (completedAt != null) {
            return Duration.between(startedAt, completedAt);
        }
        return Duration.between(startedAt, Instant.now());
    }

    /** Check if operation is still in progress. */
    public boolean isInProgress() {
        return status.isInProgress();
    }

    /** Check if operation completed successfully. */
    public boolean isSuccess() {
        return status.isSuccess();
    }

    /** Check if operation had any failures. */
    public boolean hasFailures() {
        return status.hasFailures();
    }

    /** Create a new builder for OperationRecord. */
    public static Builder builder() {
        return new Builder();
    }

    /** Create a builder initialized from this record (for updates). */
    public Builder toBuilder() {
        return new Builder()
                .id(id)
                .catalog(catalog)
                .namespace(namespace)
                .tableName(tableName)
                .policyName(policyName)
                .policyId(policyId)
                .status(status)
                .startedAt(startedAt)
                .completedAt(completedAt)
                .results(results)
                .errorMessage(errorMessage)
                .createdAt(createdAt);
    }

    public static class Builder {

        private UUID id;
        private String catalog;
        private String namespace;
        private String tableName;
        private String policyName;
        private UUID policyId;
        private OperationStatus status = OperationStatus.PENDING;
        private Instant startedAt;
        private Instant completedAt;
        private OperationResults results;
        private String errorMessage;
        private Instant createdAt;

        public Builder id(UUID id) {
            this.id = id;
            return this;
        }

        public Builder catalog(String catalog) {
            this.catalog = catalog;
            return this;
        }

        public Builder namespace(String namespace) {
            this.namespace = namespace;
            return this;
        }

        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder policyName(String policyName) {
            this.policyName = policyName;
            return this;
        }

        public Builder policyId(UUID policyId) {
            this.policyId = policyId;
            return this;
        }

        public Builder status(OperationStatus status) {
            this.status = status;
            return this;
        }

        public Builder startedAt(Instant startedAt) {
            this.startedAt = startedAt;
            return this;
        }

        public Builder completedAt(Instant completedAt) {
            this.completedAt = completedAt;
            return this;
        }

        public Builder results(OperationResults results) {
            this.results = results;
            return this;
        }

        public Builder errorMessage(String errorMessage) {
            this.errorMessage = errorMessage;
            return this;
        }

        public Builder createdAt(Instant createdAt) {
            this.createdAt = createdAt;
            return this;
        }

        public OperationRecord build() {
            Objects.requireNonNull(catalog, "catalog is required");
            Objects.requireNonNull(namespace, "namespace is required");
            Objects.requireNonNull(tableName, "tableName is required");
            Objects.requireNonNull(status, "status is required");

            if (id == null) {
                id = UUID.randomUUID();
            }
            if (startedAt == null) {
                startedAt = Instant.now();
            }
            if (createdAt == null) {
                createdAt = Instant.now();
            }

            return new OperationRecord(
                    id,
                    catalog,
                    namespace,
                    tableName,
                    policyName,
                    policyId,
                    status,
                    startedAt,
                    completedAt,
                    results,
                    errorMessage,
                    createdAt);
        }
    }
}
