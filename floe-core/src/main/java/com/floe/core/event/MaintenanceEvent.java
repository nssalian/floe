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

package com.floe.core.event;

import com.floe.core.catalog.TableIdentifier;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;

/**
 * Event emitted during maintenance operations.
 *
 * @param type event type
 * @param operationId operation or policy ID
 * @param catalog catalog name
 * @param table target table
 * @param operation operation name
 * @param status event status
 * @param timestamp when event occurred
 * @param errorMessage error message if failed
 * @param metadata additional event data
 */
public record MaintenanceEvent(
        EventType type,
        String operationId,
        String catalog,
        TableIdentifier table,
        String operation,
        EventStatus status,
        Instant timestamp,
        Optional<String> errorMessage,
        Map<String, Object> metadata) {
    public enum EventType {
        OPERATION_STARTED,
        OPERATION_SUCCEEDED,
        OPERATION_FAILED,
        POLICY_CREATED,
        POLICY_UPDATED,
        POLICY_DELETED,
    }

    public enum EventStatus {
        STARTED,
        SUCCEEDED,
        FAILED,
    }

    /** Create an operation started event. */
    public static MaintenanceEvent operationStarted(
            String operationId, String catalog, TableIdentifier table, String operation) {
        return new MaintenanceEvent(
                EventType.OPERATION_STARTED,
                operationId,
                catalog,
                table,
                operation,
                EventStatus.STARTED,
                Instant.now(),
                Optional.empty(),
                Map.of());
    }

    /** Create an operation completed event. */
    public static MaintenanceEvent operationCompleted(
            String operationId,
            String catalog,
            TableIdentifier table,
            String operation,
            Map<String, Object> metadata) {
        return new MaintenanceEvent(
                EventType.OPERATION_SUCCEEDED,
                operationId,
                catalog,
                table,
                operation,
                EventStatus.SUCCEEDED,
                Instant.now(),
                Optional.empty(),
                metadata);
    }

    /** Create an operation failed event. */
    public static MaintenanceEvent operationFailed(
            String operationId,
            String catalog,
            TableIdentifier table,
            String operation,
            String errorMessage) {
        return new MaintenanceEvent(
                EventType.OPERATION_FAILED,
                operationId,
                catalog,
                table,
                operation,
                EventStatus.FAILED,
                Instant.now(),
                Optional.of(errorMessage),
                Map.of());
    }

    /** Create a policy created event. */
    public static MaintenanceEvent policyCreated(String policyId, Map<String, Object> metadata) {
        return new MaintenanceEvent(
                EventType.POLICY_CREATED,
                policyId,
                null,
                null,
                null,
                EventStatus.SUCCEEDED,
                Instant.now(),
                Optional.empty(),
                metadata);
    }

    /** Create a policy updated event. */
    public static MaintenanceEvent policyUpdated(String policyId, Map<String, Object> metadata) {
        return new MaintenanceEvent(
                EventType.POLICY_UPDATED,
                policyId,
                null,
                null,
                null,
                EventStatus.SUCCEEDED,
                Instant.now(),
                Optional.empty(),
                metadata);
    }

    /** Create a policy deleted event. */
    public static MaintenanceEvent policyDeleted(String policyId) {
        return new MaintenanceEvent(
                EventType.POLICY_DELETED,
                policyId,
                null,
                null,
                null,
                EventStatus.SUCCEEDED,
                Instant.now(),
                Optional.empty(),
                Map.of());
    }
}
