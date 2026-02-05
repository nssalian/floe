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

package com.floe.server.resource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.when;

import com.floe.core.operation.OperationRecord;
import com.floe.core.operation.OperationResults;
import com.floe.core.operation.OperationStatus;
import com.floe.server.api.OperationResponse;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class OperationsResourceMetricsTest {

    @Mock com.floe.core.operation.OperationStore operationStore;

    @InjectMocks OperationsResource resource;

    private OperationRecord record;

    @BeforeEach
    void setUp() {
        OperationResults.SingleOperationResult opResult =
                new OperationResults.SingleOperationResult(
                        com.floe.core.maintenance.MaintenanceOperation.Type.REWRITE_DATA_FILES,
                        "SUCCEEDED",
                        1000,
                        Map.of("bytesRewritten", 10),
                        null);
        OperationResults results =
                new OperationResults(List.of(opResult), Map.of("bytesRewritten", 10));

        record =
                OperationRecord.builder()
                        .id(UUID.randomUUID())
                        .catalog("demo")
                        .namespace("db")
                        .tableName("table")
                        .engineType("SPARK")
                        .executionId("exec-1")
                        .status(OperationStatus.SUCCESS)
                        .startedAt(Instant.now())
                        .completedAt(Instant.now())
                        .results(results)
                        .normalizedMetrics(Map.of("bytesRewritten", 10))
                        .build();
    }

    @Test
    void getOperationIncludesMetrics() {
        when(operationStore.findById(record.id())).thenReturn(Optional.of(record));

        var response = resource.getById(record.id().toString());

        OperationResponse body = (OperationResponse) response.getEntity();
        assertNotNull(body);
        assertNotNull(body.results());
        assertEquals(10, ((Number) body.normalizedMetrics().get("bytesRewritten")).intValue());
        assertEquals("SPARK", body.engineType());
        assertEquals("exec-1", body.executionId());
    }
}
