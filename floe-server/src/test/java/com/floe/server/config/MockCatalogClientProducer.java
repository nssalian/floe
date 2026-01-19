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
