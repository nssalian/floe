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

package com.floe.core.operation;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class OperationRecordTest {

    // Record Tests

    @Test
    void shouldCreateRecordWithAllFields() {
        UUID id = UUID.randomUUID();
        UUID policyId = UUID.randomUUID();
        Instant startedAt = Instant.now().minus(5, ChronoUnit.MINUTES);
        Instant completedAt = Instant.now();
        Instant createdAt = startedAt;
        OperationResults results = OperationResults.empty();

        OperationRecord record =
                new OperationRecord(
                        id,
                        "demo",
                        "test",
                        "events",
                        "cleanup-policy",
                        policyId,
                        "SPARK",
                        "exec-123",
                        "sched-123",
                        "v1",
                        OperationStatus.SUCCESS,
                        startedAt,
                        completedAt,
                        results,
                        Map.of(),
                        null,
                        createdAt);

        assertEquals(id, record.id());
        assertEquals("demo", record.catalog());
        assertEquals("test", record.namespace());
        assertEquals("events", record.tableName());
        assertEquals("cleanup-policy", record.policyName());
        assertEquals(policyId, record.policyId());
        assertEquals("SPARK", record.engineType());
        assertEquals("exec-123", record.executionId());
        assertEquals("sched-123", record.scheduleId());
        assertEquals("v1", record.policyVersion());
        assertEquals(OperationStatus.SUCCESS, record.status());
        assertEquals(startedAt, record.startedAt());
        assertEquals(completedAt, record.completedAt());
        assertEquals(results, record.results());
        assertEquals(Map.of(), record.normalizedMetrics());
        assertNull(record.errorMessage());
        assertEquals(createdAt, record.createdAt());
    }

    // qualifiedTableName Tests

    @Test
    void shouldReturnQualifiedTableName() {
        OperationRecord record =
                OperationRecord.builder()
                        .catalog("demo")
                        .namespace("test")
                        .tableName("events")
                        .build();

        assertEquals("demo.test.events", record.qualifiedTableName());
    }

    // duration Tests

    @Test
    void shouldCalculateDurationForCompletedOperation() {
        Instant startedAt = Instant.now().minus(5, ChronoUnit.MINUTES);
        Instant completedAt = Instant.now();

        OperationRecord record =
                OperationRecord.builder()
                        .catalog("demo")
                        .namespace("test")
                        .tableName("events")
                        .startedAt(startedAt)
                        .completedAt(completedAt)
                        .build();

        Duration duration = record.duration();
        assertTrue(duration.toMinutes() >= 4 && duration.toMinutes() <= 6);
    }

    @Test
    void shouldCalculateDurationForRunningOperation() {
        Instant startedAt = Instant.now().minus(2, ChronoUnit.MINUTES);

        OperationRecord record =
                OperationRecord.builder()
                        .catalog("demo")
                        .namespace("test")
                        .tableName("events")
                        .status(OperationStatus.RUNNING)
                        .startedAt(startedAt)
                        .completedAt(null)
                        .build();

        Duration duration = record.duration();
        assertTrue(duration.toMinutes() >= 1);
    }

    // isInProgress Tests

    @Test
    void shouldBeInProgressWhenPending() {
        OperationRecord record =
                OperationRecord.builder()
                        .catalog("demo")
                        .namespace("test")
                        .tableName("events")
                        .status(OperationStatus.PENDING)
                        .build();

        assertTrue(record.isInProgress());
    }

    @Test
    void shouldBeInProgressWhenRunning() {
        OperationRecord record =
                OperationRecord.builder()
                        .catalog("demo")
                        .namespace("test")
                        .tableName("events")
                        .status(OperationStatus.RUNNING)
                        .build();

        assertTrue(record.isInProgress());
    }

    @Test
    void shouldNotBeInProgressWhenSuccess() {
        OperationRecord record =
                OperationRecord.builder()
                        .catalog("demo")
                        .namespace("test")
                        .tableName("events")
                        .status(OperationStatus.SUCCESS)
                        .build();

        assertFalse(record.isInProgress());
    }

    // isSuccess Tests

    @Test
    void shouldBeSuccessWhenStatusIsSuccess() {
        OperationRecord record =
                OperationRecord.builder()
                        .catalog("demo")
                        .namespace("test")
                        .tableName("events")
                        .status(OperationStatus.SUCCESS)
                        .build();

        assertTrue(record.isSuccess());
    }

    @Test
    void shouldNotBeSuccessWhenStatusIsFailed() {
        OperationRecord record =
                OperationRecord.builder()
                        .catalog("demo")
                        .namespace("test")
                        .tableName("events")
                        .status(OperationStatus.FAILED)
                        .build();

        assertFalse(record.isSuccess());
    }

    // hasFailures Tests

    @Test
    void shouldHaveFailuresWhenFailed() {
        OperationRecord record =
                OperationRecord.builder()
                        .catalog("demo")
                        .namespace("test")
                        .tableName("events")
                        .status(OperationStatus.FAILED)
                        .build();

        assertTrue(record.hasFailures());
    }

    @Test
    void shouldHaveFailuresWhenPartialFailure() {
        OperationRecord record =
                OperationRecord.builder()
                        .catalog("demo")
                        .namespace("test")
                        .tableName("events")
                        .status(OperationStatus.PARTIAL_FAILURE)
                        .build();

        assertTrue(record.hasFailures());
    }

    @Test
    void shouldNotHaveFailuresWhenSuccess() {
        OperationRecord record =
                OperationRecord.builder()
                        .catalog("demo")
                        .namespace("test")
                        .tableName("events")
                        .status(OperationStatus.SUCCESS)
                        .build();

        assertFalse(record.hasFailures());
    }

    // Builder Tests

    @Test
    void builderShouldGenerateIdIfNotProvided() {
        OperationRecord record =
                OperationRecord.builder()
                        .catalog("demo")
                        .namespace("test")
                        .tableName("events")
                        .build();

        assertNotNull(record.id());
    }

    @Test
    void builderShouldUseProvidedId() {
        UUID id = UUID.randomUUID();

        OperationRecord record =
                OperationRecord.builder()
                        .id(id)
                        .catalog("demo")
                        .namespace("test")
                        .tableName("events")
                        .build();

        assertEquals(id, record.id());
    }

    @Test
    void builderShouldSetDefaultStatusToPending() {
        OperationRecord record =
                OperationRecord.builder()
                        .catalog("demo")
                        .namespace("test")
                        .tableName("events")
                        .build();

        assertEquals(OperationStatus.PENDING, record.status());
    }

    @Test
    void builderShouldSetStartedAtIfNotProvided() {
        Instant before = Instant.now();

        OperationRecord record =
                OperationRecord.builder()
                        .catalog("demo")
                        .namespace("test")
                        .tableName("events")
                        .build();

        Instant after = Instant.now();

        assertNotNull(record.startedAt());
        assertFalse(record.startedAt().isBefore(before));
        assertFalse(record.startedAt().isAfter(after));
    }

    @Test
    void builderShouldSetCreatedAtIfNotProvided() {
        Instant before = Instant.now();

        OperationRecord record =
                OperationRecord.builder()
                        .catalog("demo")
                        .namespace("test")
                        .tableName("events")
                        .build();

        Instant after = Instant.now();

        assertNotNull(record.createdAt());
        assertFalse(record.createdAt().isBefore(before));
        assertFalse(record.createdAt().isAfter(after));
    }

    @Test
    void builderShouldThrowWhenCatalogIsNull() {
        OperationRecord.Builder builder =
                OperationRecord.builder().namespace("test").tableName("events");

        assertThrows(NullPointerException.class, builder::build);
    }

    @Test
    void builderShouldThrowWhenNamespaceIsNull() {
        OperationRecord.Builder builder =
                OperationRecord.builder().catalog("demo").tableName("events");

        assertThrows(NullPointerException.class, builder::build);
    }

    @Test
    void builderShouldThrowWhenTableNameIsNull() {
        OperationRecord.Builder builder =
                OperationRecord.builder().catalog("demo").namespace("test");

        assertThrows(NullPointerException.class, builder::build);
    }

    @Test
    void builderShouldSetAllFields() {
        UUID id = UUID.randomUUID();
        UUID policyId = UUID.randomUUID();
        Instant startedAt = Instant.now().minus(10, ChronoUnit.MINUTES);
        Instant completedAt = Instant.now();
        Instant createdAt = startedAt;
        OperationResults results = OperationResults.empty();

        OperationRecord record =
                OperationRecord.builder()
                        .id(id)
                        .catalog("demo")
                        .namespace("test")
                        .tableName("events")
                        .policyName("cleanup")
                        .policyId(policyId)
                        .status(OperationStatus.SUCCESS)
                        .startedAt(startedAt)
                        .completedAt(completedAt)
                        .results(results)
                        .errorMessage("test error")
                        .createdAt(createdAt)
                        .build();

        assertEquals(id, record.id());
        assertEquals("demo", record.catalog());
        assertEquals("test", record.namespace());
        assertEquals("events", record.tableName());
        assertEquals("cleanup", record.policyName());
        assertEquals(policyId, record.policyId());
        assertEquals(OperationStatus.SUCCESS, record.status());
        assertEquals(startedAt, record.startedAt());
        assertEquals(completedAt, record.completedAt());
        assertEquals(results, record.results());
        assertEquals("test error", record.errorMessage());
        assertEquals(createdAt, record.createdAt());
    }

    // toBuilder Tests

    @Test
    void toBuilderShouldCopyAllFields() {
        UUID id = UUID.randomUUID();
        UUID policyId = UUID.randomUUID();
        Instant startedAt = Instant.now().minus(10, ChronoUnit.MINUTES);
        Instant completedAt = Instant.now();
        OperationResults results = OperationResults.empty();

        OperationRecord original =
                OperationRecord.builder()
                        .id(id)
                        .catalog("demo")
                        .namespace("test")
                        .tableName("events")
                        .policyName("cleanup")
                        .policyId(policyId)
                        .status(OperationStatus.SUCCESS)
                        .startedAt(startedAt)
                        .completedAt(completedAt)
                        .results(results)
                        .errorMessage("error")
                        .createdAt(startedAt)
                        .build();

        OperationRecord copy = original.toBuilder().build();

        assertEquals(original.id(), copy.id());
        assertEquals(original.catalog(), copy.catalog());
        assertEquals(original.namespace(), copy.namespace());
        assertEquals(original.tableName(), copy.tableName());
        assertEquals(original.policyName(), copy.policyName());
        assertEquals(original.policyId(), copy.policyId());
        assertEquals(original.status(), copy.status());
        assertEquals(original.startedAt(), copy.startedAt());
        assertEquals(original.completedAt(), copy.completedAt());
        assertEquals(original.results(), copy.results());
        assertEquals(original.errorMessage(), copy.errorMessage());
        assertEquals(original.createdAt(), copy.createdAt());
    }

    @Test
    void toBuilderShouldAllowModification() {
        OperationRecord original =
                OperationRecord.builder()
                        .catalog("demo")
                        .namespace("test")
                        .tableName("events")
                        .status(OperationStatus.RUNNING)
                        .build();

        OperationRecord updated =
                original.toBuilder()
                        .status(OperationStatus.SUCCESS)
                        .completedAt(Instant.now())
                        .build();

        assertEquals(OperationStatus.RUNNING, original.status());
        assertEquals(OperationStatus.SUCCESS, updated.status());
        assertNotNull(updated.completedAt());
    }
}
