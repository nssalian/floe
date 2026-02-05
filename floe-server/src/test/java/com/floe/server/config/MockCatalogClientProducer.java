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

package com.floe.server.config;

import com.floe.core.catalog.CatalogClient;
import com.floe.core.catalog.TableIdentifier;
import com.floe.core.catalog.TableMetadata;
import io.quarkus.test.Mock;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import java.util.List;
import java.util.Optional;
import org.apache.iceberg.Table;

/**
 * Mock CatalogClient producer for Quarkus tests.
 *
 * <p>Provides a catalog with test tables for policy validation.
 */
@Mock
@ApplicationScoped
public class MockCatalogClientProducer {

    @Produces
    @ApplicationScoped
    public CatalogClient produceMockCatalogClient() {
        return new MockCatalogClient();
    }

    private static class MockCatalogClient implements CatalogClient {

        @Override
        public String getCatalogName() {
            return "demo";
        }

        @Override
        public boolean isHealthy() {
            return true;
        }

        @Override
        public List<String> listNamespaces() {
            return List.of("test");
        }

        @Override
        public List<TableIdentifier> listTables(String namespace) {
            if ("test".equals(namespace)) {
                return List.of(
                        TableIdentifier.of("demo", "test", "events"),
                        TableIdentifier.of("demo", "test", "users"),
                        TableIdentifier.of("demo", "test", "orders"));
            }
            return List.of();
        }

        @Override
        public List<TableIdentifier> listAllTables() {
            return List.of(
                    TableIdentifier.of("demo", "test", "events"),
                    TableIdentifier.of("demo", "test", "users"),
                    TableIdentifier.of("demo", "test", "orders"));
        }

        @Override
        public Optional<Table> loadTable(TableIdentifier identifier) {
            return Optional.empty();
        }

        @Override
        public Optional<TableMetadata> getTableMetadata(TableIdentifier identifier) {
            return Optional.empty();
        }

        @Override
        public void close() {
            // No-op
        }
    }
}
